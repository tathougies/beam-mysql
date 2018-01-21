-- | Beam defines a custom 'FromField' type class for types that can be read from
--   MySQL fields. The ones in 'mysql-simple' are inadequate because they rely on
--   bizarre asynchronous exceptions that cannot be used consistently

module Database.Beam.MySQL.FromField where

import           Database.MySQL.Base
import           Database.MySQL.Base.Types

import           Control.Exception
import           Control.Monad.Except

import           Data.Attoparsec.ByteString.Char8
import qualified Data.ByteString.Char8 as SB
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int
import           Data.Proxy
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as TS
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Encoding as TE
import           Data.Typeable
import           Data.Word

type FieldParser a = ExceptT ParseError IO a

data ParseError
    = ParseError
    { _parseErrorSqlType     :: String
    , _parseErrorHaskellType :: String
    , _parseErrorMessage     :: String
    , _parseErrorType        :: ParseErrorType
    } deriving Show
instance Exception ParseError

data ParseErrorType
    = Incompatible | UnexpectedNull | ConversionFailed
      deriving Show

class FromField a where
    fromField :: Field -> Maybe SB.ByteString -> FieldParser a

instance FromField Bool where
    fromField f d = (/= (0::Word8)) <$> fromField f d

instance FromField Word where
    fromField = atto check64 decimal
instance FromField Word8 where
    fromField = atto check8 decimal
instance FromField Word16 where
    fromField = atto check16 decimal
instance FromField Word32 where
    fromField = atto check32 decimal
instance FromField Word64 where
    fromField = atto check64 decimal

instance FromField Int where
    fromField = atto check64 (signed decimal)
instance FromField Int8 where
    fromField = atto check8 (signed decimal)
instance FromField Int16 where
    fromField = atto check16 (signed decimal)
instance FromField Int32 where
    fromField = atto check32 (signed decimal)
instance FromField Int64 where
    fromField = atto check64 (signed decimal)

instance FromField Float where
    fromField = atto check (realToFrac <$> double)
      where
        check ty | check16 ty = True
        check Int24 = True
        check Float = True
        check Decimal = True
        check NewDecimal = True
        check Double = True
        check _= False

instance FromField Double where
    fromField = atto check double
      where
        check ty | check32 ty = True
        check Float = True
        check Double = True
        check Decimal = True
        check NewDecimal = True
        check _ = False

instance FromField Scientific where
    fromField = atto checkScientific rational

instance FromField (Ratio Integer) where
    fromField = atto checkScientific rational

instance FromField a => FromField (Maybe a) where
    fromField _ Nothing = pure Nothing
    fromField field (Just d) = Just <$> fromField field (Just d)

instance FromField SB.ByteString where
    fromField = doConvert checkText pure

instance FromField LB.ByteString where
    fromField f d = fmap (LB.fromChunks . pure) (fromField f d)

instance FromField TS.Text where
    fromField = doConvert checkText (either (Left . show) Right . TE.decodeUtf8')

instance FromField TL.Text where
    fromField f d = fmap (TL.fromChunks . pure) (fromField f d)

incompatibleTypes, unexpectedNull, conversionFailed
    :: forall a. Typeable a => Field -> String -> FieldParser a
incompatibleTypes f msg =
  throwError (ParseError (show (fieldType f)) (show (typeRep (Proxy :: Proxy a)))
                         msg Incompatible)
unexpectedNull f msg =
  throwError (ParseError (show (fieldType f)) (show (typeRep (Proxy :: Proxy a)))
                         msg UnexpectedNull)
conversionFailed f msg =
  throwError (ParseError (show (fieldType f)) (show (typeRep (Proxy :: Proxy a)))
                         msg ConversionFailed)

check8, check16, check32, check64, checkScientific, checkBytes, checkText
    :: Type -> Bool

check8 Tiny = True
check8 NewDecimal = True
check8 _ = False

check16 ty | check8 ty = True
check16 Short = True
check16 _= False

check32 ty | check16 ty = True
check32 Int24 = True
check32 Long = True
check32 _ = False

check64 ty | check32 ty = True
check64 LongLong = True
check64 _ = False

checkScientific ty | check64 ty = True
checkScientific Float = True
checkScientific Double = True
checkScientific Decimal = True
checkScientific NewDecimal = True
checkScientific _ = True

checkBytes ty | checkText ty = True
checkBytes TinyBlob = True
checkBytes MediumBlob = True
checkBytes LongBlob = True
checkBytes Blob = True
checkBytes _ = False

checkText VarChar = True
checkText VarString = True
checkText String = True
checkText Enum = True
checkText _ = False

doConvert :: Typeable a => (Type -> Bool)
          -> (SB.ByteString -> Either String a)
          -> Field -> Maybe SB.ByteString -> FieldParser a
doConvert _ _ f Nothing = unexpectedNull f ""
doConvert checkType parser field (Just d)
  | checkType (fieldType field) =
      case parser d of
        Left err -> conversionFailed field err
        Right r -> pure r
  | otherwise = incompatibleTypes field ""

atto :: Typeable a => (Type -> Bool) -> Parser a
     -> Field -> Maybe SB.ByteString -> FieldParser a
atto checkType parser =
  doConvert checkType (parseOnly parser)
