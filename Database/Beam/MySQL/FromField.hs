-- | Beam defines a custom 'FromField' type class for types that can be read from
--   MySQL fields. The ones in 'mysql-simple' are inadequate because they rely on
--   bizarre asynchronous exceptions that cannot be used consistently

{-# LANGUAGE BangPatterns #-}

module Database.Beam.MySQL.FromField
    ( FieldParser
    , FromField(..)

    , atto ) where

import           Database.Beam.Backend.SQL (SqlNull(..))
import           Database.Beam.Backend.SQL.Row (ColumnParseError(..))

import           Database.MySQL.Base
import           Database.MySQL.Base.Types

import           Control.Applicative
import           Control.Monad.Except

import qualified Data.Aeson as A (Value, eitherDecodeStrict)
import           Data.Attoparsec.ByteString.Char8
import qualified Data.ByteString.Char8 as SB
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Char
import           Data.Fixed
import           Data.Int
import           Data.Proxy
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as TS
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import           Data.Time
import           Data.Typeable
import           Data.Word

import           Text.Printf

type FieldParser a = ExceptT ColumnParseError IO a

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

instance FromField SqlNull where
    fromField _ Nothing = pure SqlNull
    fromField f _ = throwError (ColumnTypeMismatch "SqlNull"
                                                   (show (fieldType f))
                                                   "Non-null value found")

instance FromField SB.ByteString where
    fromField = doConvert checkBytes pure

instance FromField LB.ByteString where
    fromField f d = fmap (LB.fromChunks . pure) (fromField f d)

instance FromField TS.Text where
    fromField = doConvert checkText (either (Left . show) Right . TE.decodeUtf8')

instance FromField TL.Text where
    fromField f d = fmap (TL.fromChunks . pure) (fromField f d)

instance FromField LocalTime where
    fromField = atto checkDate localTime
      where
        checkDate DateTime = True
        checkDate Timestamp = True
        checkDate Date = True
        checkDate _ = False

        localTime = do
          (day, time) <- dayAndTime
          pure (LocalTime day time)

instance FromField Day where
    fromField = atto checkDay dayP
      where
        checkDay Date = True
        checkDay _    = False

instance FromField TimeOfDay where
    fromField = atto checkTime timeP
      where
        checkTime Time = True
        checkTime _ = False

instance FromField NominalDiffTime where
    fromField = atto checkTime durationP
      where
        checkTime Time = True
        checkTime _ = False

instance FromField A.Value where
    fromField f bs =
        case (maybeToRight "Failed to extract JSON bytes." bs) >>= A.eitherDecodeStrict of
            Left err -> conversionFailed f err
            Right x -> pure x

dayAndTime :: Parser (Day, TimeOfDay)
dayAndTime = do
  day <- dayP
  _ <- char ' '
  time <- timeP

  pure (day, time)

timeP :: Parser TimeOfDay
timeP = do
  hour <- lengthedDecimal 2
  _ <- char ':'
  minute <- lengthedDecimal 2
  _ <- char ':'
  seconds <- lengthedDecimal 2
  microseconds <- (char '.' *> maxLengthedDecimal 6) <|>
                  pure 0

  let pico = seconds + microseconds * 1e-6
  case makeTimeOfDayValid hour minute pico of
    Nothing -> fail (printf "Invalid time part: %02d:%02d:%s" hour minute (showFixed False pico))
    Just tod -> pure tod

durationP :: Parser NominalDiffTime
durationP = do
  negative <- (True <$ char '-') <|> pure False
  hour <- lengthedDecimal 3
  _ <- char ':'
  minute <- lengthedDecimal 2
  _ <- char ':'
  seconds <- lengthedDecimal 2
  microseconds <- (char '.' *> maxLengthedDecimal 6) <|>
                  pure 0

  let v = hour * 3600 + minute * 60 + seconds +
          microseconds * 1e-6

  pure (if negative then negate v else v)

dayP :: Parser Day
dayP = do
  year <- lengthedDecimal 4
  _ <- char '-'
  month <- lengthedDecimal 2
  _ <- char '-'
  day <- lengthedDecimal 2

  case fromGregorianValid year month day of
    Nothing -> fail (printf "Invalid date part: %04d-%02d-%02d" year month day)
    Just day' -> pure day'

lengthedDecimal :: Num a => Int -> Parser a
lengthedDecimal = lengthedDecimal' 0
  where
    lengthedDecimal' !a 0 = pure a
    lengthedDecimal' !a n = do
      d <- digitToInt <$> digit
      lengthedDecimal' (a * 10 + fromIntegral d) (n - 1)

maxLengthedDecimal :: Num a => Int -> Parser a
maxLengthedDecimal = go1 0
  where
    go1 a n = do
      d <- digitToInt <$> digit
      go' (a * 10 + fromIntegral d) (n - 1)

    go' !a 0 = pure a
    go' !a n =
      go1 a n <|> pure (a * 10  ^ n)

incompatibleTypes, unexpectedNull, conversionFailed
    :: forall a. Typeable a => Field -> String -> FieldParser a
incompatibleTypes f msg =
  throwError (ColumnTypeMismatch (show (typeRep (Proxy :: Proxy a)))
                                 (show (fieldType f))
                                 msg)
unexpectedNull _ _ =
  throwError ColumnUnexpectedNull
conversionFailed f msg =
  throwError (ColumnTypeMismatch (show (typeRep (Proxy :: Proxy a)))
                                 (show (fieldType f))
                                 msg)

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

maybeToRight :: b -> Maybe a -> Either b a
maybeToRight _ (Just x) = Right x
maybeToRight y Nothing  = Left y
