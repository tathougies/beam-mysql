{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}

module Database.Beam.MySQL.Connection
    ( MySQL(..), MySQL.Connection

    , MySQL.connect, MySQL.close

    , mysqlUriSyntax ) where

import           Database.Beam.MySQL.Syntax
import           Database.Beam.MySQL.FromField

import           Database.Beam.Backend.SQL
import           Database.Beam.Backend.URI

import           Database.MySQL.Base as MySQL
import qualified Database.MySQL.Base.Types as MySQL

import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Free.Church
import           Control.Monad.Reader

import           Data.ByteString.Builder
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.List
import           Data.Maybe
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import           Data.Word

import           Network.URI

import           Text.Read hiding (step)

data MySQL = MySQL

instance BeamBackend MySQL where
    type BackendFromField MySQL = FromField

newtype MySQLM a = MySQLM (ReaderT (String -> IO (), Connection) IO a)
    deriving (Monad, MonadIO, Applicative, Functor)
data NotEnoughColumns
    = NotEnoughColumns
    { _errColCount :: Int
    } deriving Show

instance Exception NotEnoughColumns where
    displayException (NotEnoughColumns colCnt) =
        mconcat [ "Not enough columns while reading MySQL row. Only have "
                , show colCnt, " column(s)" ]

instance MonadBeam MysqlCommandSyntax MySQL Connection MySQLM where
    withDatabase conn action =
      withMySQL (\_ -> pure ()) conn action
    withDatabaseDebug dbg conn action =
      withMySQL dbg conn action

    runReturningMany (MysqlCommandSyntax (MysqlSyntax cmd))
                     (consume :: MySQLM (Maybe x) -> MySQLM a) =
        MySQLM . ReaderT $ \(dbg, conn) -> do
          cmdBuilder <- cmd (\_ b _ -> pure b) (MySQL.escape conn) mempty conn
          let cmdStr = BL.toStrict (toLazyByteString cmdBuilder)

          dbg (BS.unpack cmdStr)

          MySQL.query conn cmdStr

          bracket (useResult conn) freeResult $ \res -> do
            fieldDescs <- MySQL.fetchFields res

            let fetchRow' :: MySQLM (Maybe x)
                fetchRow' =
                  MySQLM . ReaderT $ \_ -> do
                    fields <- MySQL.fetchRow res

                    case fields of
                      [] -> pure Nothing
                      _ -> Just <$> runF fromBackendRow (\x _ _ -> pure x) step
                                         0 (zip fieldDescs fields)

                parseField :: forall field. FromField field
                           => MySQL.Field -> Maybe BS.ByteString
                           -> IO (Either ParseError field)
                parseField ty d = runExceptT (fromField ty d)

                step :: FromBackendRowF MySQL (Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO x)
                     -> Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO x
                step (ParseOneField _) curCol [] =
                    throwIO (NotEnoughColumns (curCol + 1))
                step (ParseOneField next) curCol ((desc, field):fields) =
                    do d <- parseField desc field
                       case d of
                         Left  e  -> throwIO e
                         Right d' -> next d' (curCol + 1) fields
                step (PeekField next) curCol fields@((desc, field):_) =
                    do d <- parseField desc field
                       case d of
                         Left {}  -> next Nothing curCol fields
                         Right d' -> next (Just d') curCol fields
                step (PeekField next) curCol [] =
                    next Nothing curCol []
                step (CheckNextNNull n next) curCol fields
                    | n > length fields = next False curCol fields
                    | otherwise =
                        let areNull = all (isNothing . snd) (take n fields)
                        in next areNull
                                (if areNull then curCol + n else curCol)
                                (if areNull then drop n fields else fields)

                MySQLM doConsume = consume fetchRow'

            runReaderT doConsume (dbg, conn)

withMySQL :: (String -> IO ()) -> Connection
          -> MySQLM a -> IO a
withMySQL dbg conn (MySQLM a) =
    runReaderT a (dbg, conn)

mysqlUriSyntax :: c MysqlCommandSyntax MySQL Connection MySQLM
               -> BeamURIOpeners c
mysqlUriSyntax =
    mkUriOpener "mysql:"
        (\uri action ->
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
             in bracket (connect connInfo) close action)

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
