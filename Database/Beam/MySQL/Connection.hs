{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}

module Database.Beam.MySQL.Connection
    ( MySQL(..), MySQL.Connection
    , MySQLM(..)

    , runBeamMySQL, runBeamMySQLDebug

    , MysqlCommandSyntax(..)
    , MysqlSelectSyntax(..), MysqlInsertSyntax(..)
    , MysqlUpdateSyntax(..), MysqlDeleteSyntax(..)
    , MysqlExpressionSyntax(..)

    , MySQL.connect, MySQL.close

    , mysqlUriSyntax ) where

import           Database.Beam.MySQL.Syntax
import           Database.Beam.MySQL.FromField

import           Database.Beam.Backend
import           Database.Beam.Backend.URI
import           Database.Beam.Query
import           Database.Beam.Query.SQL92

import           Database.MySQL.Base as MySQL
import qualified Database.MySQL.Base.Types as MySQL

import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Fail (MonadFail)
import qualified Control.Monad.Fail as Fail
import           Control.Monad.Free.Church
import           Control.Monad.Reader

import qualified Data.Aeson as A (Value)
import           Data.ByteString.Builder
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.List
import           Data.Maybe
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import           Data.Time (Day, LocalTime, NominalDiffTime, TimeOfDay)
import           Data.Word

import           Network.URI

import           Text.Read hiding (step)

data MySQL = MySQL

instance BeamSqlBackendIsString MySQL String
instance BeamSqlBackendIsString MySQL T.Text

instance BeamBackend MySQL where
    type BackendFromField MySQL = FromField

instance BeamSqlBackend MySQL
type instance BeamSqlBackendSyntax MySQL = MysqlCommandSyntax

newtype MySQLM a = MySQLM (ReaderT (String -> IO (), Connection) IO a)
    deriving (Monad, MonadIO, Applicative, Functor)

instance MonadFail MySQLM where
    fail e = fail $ "Internal Error with: " <> show e

data NotEnoughColumns
    = NotEnoughColumns
    { _errColCount :: Int
    } deriving Show

instance Exception NotEnoughColumns where
    displayException (NotEnoughColumns colCnt) =
        mconcat [ "Not enough columns while reading MySQL row. Only have "
                , show colCnt, " column(s)" ]

data CouldNotReadColumn
  = CouldNotReadColumn
  { _errColIndex :: Int
  , _errColMsg   :: String }
  deriving Show

instance Exception CouldNotReadColumn where
  displayException (CouldNotReadColumn idx msg) =
    mconcat [ "Could not read column ", show idx, ": ", msg ]

runBeamMySQLDebug :: (String -> IO ()) -> Connection -> MySQLM a -> IO a
runBeamMySQLDebug = withMySQL

runBeamMySQL :: Connection -> MySQLM a -> IO a
runBeamMySQL = runBeamMySQLDebug (\_ -> pure ())

instance MonadBeam MySQL MySQLM where
    runReturningMany (MysqlCommandSyntax (MysqlSyntax cmd))
                     (consume :: MySQLM (Maybe x) -> MySQLM a) =
        MySQLM . ReaderT $ \(dbg, conn) -> do
          cmdBuilder <- cmd (\_ b _ -> pure b) (MySQL.escape conn) mempty conn
          let cmdStr = BL.toStrict (toLazyByteString cmdBuilder)

          dbg (T.unpack (TE.decodeUtf8 cmdStr))

          MySQL.query conn cmdStr

          bracket (useResult conn) freeResult $ \res -> do
            fieldDescs <- MySQL.fetchFields res

            let fetchRow' :: MySQLM (Maybe x)
                fetchRow' =
                  MySQLM . ReaderT $ \_ -> do
                    fields <- MySQL.fetchRow res

                    case fields of
                      [] -> pure Nothing
                      _ -> do
                        let FromBackendRowM go = fromBackendRow
                        res <- runF go (\x _ _ -> pure (Right x)) step
                                 0 (zip fieldDescs fields)
                        case res of
                          Left err -> throwIO err
                          Right x -> Just x

                parseField :: forall field. FromField field
                           => MySQL.Field -> Maybe BS.ByteString
                           -> IO (Either ParseError field)
                parseField ty d = runExceptT (fromField ty d)

                step :: FromBackendRowF MySQL (Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO (Either BeamRowReadError x))
                     -> Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO (Either BeamRowReadError x)
                step (ParseOneField _) curCol [] =
                    pure (Left (BeamRowReadError (Just curCol) (NotEnoughColumns curCol)))
                step (ParseOneField next) curCol ((desc, field):fields) =
                    do d <- parseField desc field
                       case d of
                         Left  e  -> _
                         Right d' -> next d' curCol fields

                MySQLM doConsume = consume fetchRow'

            runReaderT doConsume (dbg, conn)

withMySQL :: (String -> IO ()) -> Connection
          -> MySQLM a -> IO a
withMySQL dbg conn (MySQLM a) =
    runReaderT a (dbg, conn)

mysqlUriSyntax :: c MySQL Connection MySQLM
               -> BeamURIOpeners c
mysqlUriSyntax =
    mkUriOpener (withMySQL (const (pure ()))) "mysql:"
        (\uri ->
             let stripSuffix s a =
                     reverse <$> stripPrefix (reverse s) (reverse a)

                 (user, pw) =
                     fromMaybe ("root", "") $ do
                       userInfo  <- fmap uriUserInfo (uriAuthority uri)
                       userInfo' <- stripSuffix "@" userInfo
                       let (user', pw') = break (== ':') userInfo'
                           pw'' = fromMaybe "" (stripPrefix ":" pw')
                       pure (user', pw'')
                 host =
                     fromMaybe "localhost" .
                     fmap uriRegName . uriAuthority $ uri
                 port =
                     fromMaybe 3306 $ do
                       portStr <- fmap uriPort (uriAuthority uri)
                       portStr' <- stripPrefix ":" portStr
                       readMaybe portStr'

                 db = fromMaybe "test" $
                      stripPrefix "/" (uriPath uri)

                 options =
                   fromMaybe [CharsetName "utf-8"] $ do
                     opts <- stripPrefix "?" (uriQuery uri)
                     let getKeyValuePairs "" a = a []
                         getKeyValuePairs d a =
                             let (keyValue, d') = break (=='&') d
                                 attr = parseKeyValue keyValue
                             in getKeyValuePairs d' (a . maybe id (:) attr)

                     pure (getKeyValuePairs opts id)

                 parseBool (Just "true") = pure True
                 parseBool (Just "false") = pure False
                 parseBool _ = Nothing

                 parseKeyValue kv = do
                   let (key, value) = break (==':') kv
                       value' = stripPrefix ":" value

                   case (key, value') of
                     ("connectTimeout", Just secs) ->
                         ConnectTimeout <$> readMaybe secs
                     ( "compress", _) -> pure Compress
                     ( "namedPipe", _ ) -> pure NamedPipe
                     ( "initCommand", Just cmd ) ->
                         pure (InitCommand (BS.pack cmd))
                     ( "readDefaultFile", Just fp ) ->
                         pure (ReadDefaultFile fp)
                     ( "readDefaultGroup", Just grp ) ->
                         pure (ReadDefaultGroup (BS.pack grp))
                     ( "charsetDir", Just fp ) ->
                         pure (CharsetDir fp)
                     ( "charsetName", Just nm ) ->
                         pure (CharsetName nm)
                     ( "localInFile", b ) ->
                         LocalInFile <$> parseBool b
                     ( "protocol", Just p) ->
                         case p of
                           "tcp" -> pure (Protocol TCP)
                           "socket" -> pure (Protocol Socket)
                           "pipe" -> pure (Protocol Pipe)
                           "memory" -> pure (Protocol Memory)
                           _ -> Nothing
                     ( "sharedMemoryBaseName", Just fp ) ->
                         pure (SharedMemoryBaseName (BS.pack fp))
                     ( "readTimeout", Just secs ) ->
                         ReadTimeout <$> readMaybe secs
                     ( "writeTimeout", Just secs ) ->
                         WriteTimeout <$> readMaybe secs
                     ( "useRemoteConnection", _ ) -> pure UseRemoteConnection
                     ( "useEmbeddedConnection", _ ) -> pure UseEmbeddedConnection
                     ( "guessConnection", _ ) -> pure GuessConnection
                     ( "clientIp", Just fp) ->
                         pure (ClientIP (BS.pack fp))
                     ( "secureAuth", b ) ->
                         SecureAuth <$> parseBool b
                     ( "reportDataTruncation", b ) ->
                         ReportDataTruncation <$> parseBool b
                     ( "reconnect", b ) ->
                         Reconnect <$> parseBool b
                     ( "sslVerifyServerCert", b) ->
                         SSLVerifyServerCert <$> parseBool b
                     ( "foundRows", _ ) -> pure FoundRows
                     ( "ignoreSIGPIPE", _ ) -> pure IgnoreSIGPIPE
                     ( "ignoreSpace", _ ) -> pure IgnoreSpace
                     ( "interactive", _ ) -> pure Interactive
                     ( "localFiles", _ ) -> pure LocalFiles
                     ( "multiResults", _ ) -> pure MultiResults
                     ( "multiStatements", _ ) -> pure MultiStatements
                     ( "noSchema", _ ) -> pure NoSchema
                     _ -> Nothing

                 connInfo = ConnectInfo
                          { connectHost = host, connectPort = port
                          , connectUser = user, connectPassword = pw
                          , connectDatabase = db, connectOptions = options
                          , connectPath = "", connectSSL = Nothing }
             in connect connInfo >>= \hdl -> pure (hdl, close hdl))

#define FROM_BACKEND_ROW(ty) instance FromBackendRow MySQL ty

FROM_BACKEND_ROW(Bool)
FROM_BACKEND_ROW(Word)
FROM_BACKEND_ROW(Word8)
FROM_BACKEND_ROW(Word16)
FROM_BACKEND_ROW(Word32)
FROM_BACKEND_ROW(Word64)
FROM_BACKEND_ROW(Int)
FROM_BACKEND_ROW(Int8)
FROM_BACKEND_ROW(Int16)
FROM_BACKEND_ROW(Int32)
FROM_BACKEND_ROW(Int64)
FROM_BACKEND_ROW(Float)
FROM_BACKEND_ROW(Double)
FROM_BACKEND_ROW(Scientific)
FROM_BACKEND_ROW((Ratio Integer))
FROM_BACKEND_ROW(BS.ByteString)
FROM_BACKEND_ROW(BL.ByteString)
FROM_BACKEND_ROW(T.Text)
FROM_BACKEND_ROW(TL.Text)
FROM_BACKEND_ROW(LocalTime)
FROM_BACKEND_ROW(A.Value)

-- * Equality checks
#define HAS_MYSQL_EQUALITY_CHECK(ty)                       \
  instance HasSqlEqualityCheck MySQL (ty); \
  instance HasSqlQuantifiedEqualityCheck MySQL (ty);

HAS_MYSQL_EQUALITY_CHECK(Bool)
HAS_MYSQL_EQUALITY_CHECK(Double)
HAS_MYSQL_EQUALITY_CHECK(Float)
HAS_MYSQL_EQUALITY_CHECK(Int)
HAS_MYSQL_EQUALITY_CHECK(Int8)
HAS_MYSQL_EQUALITY_CHECK(Int16)
HAS_MYSQL_EQUALITY_CHECK(Int32)
HAS_MYSQL_EQUALITY_CHECK(Int64)
HAS_MYSQL_EQUALITY_CHECK(Integer)
HAS_MYSQL_EQUALITY_CHECK(Word)
HAS_MYSQL_EQUALITY_CHECK(Word8)
HAS_MYSQL_EQUALITY_CHECK(Word16)
HAS_MYSQL_EQUALITY_CHECK(Word32)
HAS_MYSQL_EQUALITY_CHECK(Word64)
HAS_MYSQL_EQUALITY_CHECK(T.Text)
HAS_MYSQL_EQUALITY_CHECK(TL.Text)
HAS_MYSQL_EQUALITY_CHECK([Char])
HAS_MYSQL_EQUALITY_CHECK(Scientific)
HAS_MYSQL_EQUALITY_CHECK(Day)
HAS_MYSQL_EQUALITY_CHECK(TimeOfDay)
HAS_MYSQL_EQUALITY_CHECK(NominalDiffTime)
HAS_MYSQL_EQUALITY_CHECK(LocalTime)

instance HasQBuilder MySQL where
    buildSqlQuery = buildSql92Query' True
