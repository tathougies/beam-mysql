{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Beam.MySQL.Syntax where

import           Database.Beam.Backend.SQL
import           Database.Beam.Query

import           Database.MySQL.Base (Connection)

import qualified Data.Aeson as A (Value, encode)
import           Data.ByteString (ByteString)
import           Data.ByteString.Builder
import           Data.ByteString.Builder.Scientific (scientificBuilder)
import qualified Data.ByteString.Lazy as BL (toStrict)
import           Data.Fixed
import           Data.Int
import           Data.Maybe (maybe)
import           Data.Monoid (Monoid)
import           Data.Scientific (Scientific)
import           Data.Semigroup (Semigroup, (<>))
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import           Data.Time
import           Data.Word

newtype MysqlSyntax
    = MysqlSyntax
    { fromMysqlSyntax :: forall m. Monad m
                      => ((ByteString -> m ByteString) ->
                          Builder -> Connection -> m Builder)
                      -> (ByteString -> m ByteString)
                      -> Builder -> Connection -> m Builder
    }

newtype MysqlCommandSyntax = MysqlCommandSyntax { fromMysqlCommand :: MysqlSyntax }
newtype MysqlSelectSyntax = MysqlSelectSyntax { fromMysqlSelect :: MysqlSyntax }
newtype MysqlInsertSyntax = MysqlInsertSyntax { fromMysqlInsert :: MysqlSyntax }
newtype MysqlUpdateSyntax = MysqlUpdateSyntax { fromMysqlUpdate :: MysqlSyntax }
newtype MysqlDeleteSyntax = MysqlDeleteSyntax { fromMysqlDelete :: MysqlSyntax }
newtype MysqlTableNameSyntax = MysqlTableNameSyntax { fromMysqlTableName :: MysqlSyntax }
newtype MysqlFieldNameSyntax = MysqlFieldNameSyntax { fromMysqlFieldName :: MysqlSyntax }
newtype MysqlExpressionSyntax = MysqlExpressionSyntax { fromMysqlExpression :: MysqlSyntax } deriving Eq
newtype MysqlValueSyntax = MysqlValueSyntax { fromMysqlValue :: MysqlSyntax }
newtype MysqlInsertValuesSyntax = MysqlInsertValuesSyntax { fromMysqlInsertValues :: MysqlSyntax }
newtype MysqlSelectTableSyntax = MysqlSelectTableSyntax { fromMysqlSelectTable :: MysqlSyntax }
newtype MysqlSetQuantifierSyntax = MysqlSetQuantifierSyntax { fromMysqlSetQuantifier :: MysqlSyntax }
newtype MysqlComparisonQuantifierSyntax = MysqlComparisonQuantifierSyntax { fromMysqlComparisonQuantifier :: MysqlSyntax }
newtype MysqlOrderingSyntax = MysqlOrderingSyntax { fromMysqlOrdering :: MysqlSyntax }
newtype MysqlFromSyntax = MysqlFromSyntax { fromMysqlFrom :: MysqlSyntax }
newtype MysqlGroupingSyntax = MysqlGroupingSyntax { fromMysqlGrouping :: MysqlSyntax }
newtype MysqlTableSourceSyntax = MysqlTableSourceSyntax { fromMysqlTableSource :: MysqlSyntax }
newtype MysqlProjectionSyntax = MysqlProjectionSyntax { fromMysqlProjection :: MysqlSyntax }
data MysqlDataTypeSyntax
  = MysqlDataTypeSyntax { fromMysqlDataType :: MysqlSyntax
                        , fromMysqlDataTypeCast :: MysqlSyntax }
newtype MysqlExtractFieldSyntax = MysqlExtractFieldSyntax { fromMysqlExtractField :: MysqlSyntax }

instance Eq MysqlSyntax where
    _ == _ = False

instance Semigroup MysqlSyntax where
    (<>) = mappend

instance Monoid MysqlSyntax where
    mempty = MysqlSyntax id
    mappend (MysqlSyntax a) (MysqlSyntax b) =
        MysqlSyntax $ \next -> a (b next)

emit :: Builder -> MysqlSyntax
emit b = MysqlSyntax (\next doEscape before -> next doEscape (before <> b))

escape :: ByteString -> MysqlSyntax
escape b = MysqlSyntax (\next doEscape before conn ->
                            doEscape b >>= \b' ->
                            next doEscape (before <> byteString b') conn)

-- We use backticks in MySQL, because the double quote mode requires
-- ANSI_QUOTES, which may not always be enabled
mysqlIdentifier :: Text -> MysqlSyntax
mysqlIdentifier t =
    emit "`" <>
    MysqlSyntax (\next doEscape before ->
                     next doEscape (before <> TE.encodeUtf8Builder t)) <>
    emit "`"

mysqlSepBy :: MysqlSyntax -> [MysqlSyntax] -> MysqlSyntax
mysqlSepBy _ [] = mempty
mysqlSepBy _ [a] = a
mysqlSepBy sep (a:as) = a <> foldMap (sep <>) as

mysqlParens :: MysqlSyntax -> MysqlSyntax
mysqlParens a = emit "(" <> a <> emit ")"

instance IsSql92Syntax MysqlCommandSyntax where
    type Sql92SelectSyntax MysqlCommandSyntax = MysqlSelectSyntax
    type Sql92InsertSyntax MysqlCommandSyntax = MysqlInsertSyntax
    type Sql92UpdateSyntax MysqlCommandSyntax = MysqlUpdateSyntax
    type Sql92DeleteSyntax MysqlCommandSyntax = MysqlDeleteSyntax

    selectCmd = MysqlCommandSyntax . fromMysqlSelect
    insertCmd = MysqlCommandSyntax . fromMysqlInsert
    deleteCmd = MysqlCommandSyntax . fromMysqlDelete
    updateCmd = MysqlCommandSyntax . fromMysqlUpdate

instance IsSql92UpdateSyntax MysqlUpdateSyntax where
    type Sql92UpdateFieldNameSyntax MysqlUpdateSyntax = MysqlFieldNameSyntax
    type Sql92UpdateExpressionSyntax MysqlUpdateSyntax = MysqlExpressionSyntax
    type Sql92UpdateTableNameSyntax MysqlUpdateSyntax = MysqlTableNameSyntax

    updateStmt tbl fields where_ =
      MysqlUpdateSyntax $
      emit "UPDATE " <> fromMysqlTableName tbl <>
      (case fields of
         [] -> mempty
         _ ->
           emit " SET " <>
           mysqlSepBy (emit ", ") (map (\(field, val) -> fromMysqlFieldName field <> emit "=" <>
                                                         fromMysqlExpression val) fields)) <>
      maybe mempty (\where' -> emit " WHERE " <> fromMysqlExpression where') where_

instance IsSql92InsertSyntax MysqlInsertSyntax where
    type Sql92InsertValuesSyntax MysqlInsertSyntax = MysqlInsertValuesSyntax
    type Sql92InsertTableNameSyntax MysqlInsertSyntax = MysqlTableNameSyntax

    insertStmt tblName fields values =
      MysqlInsertSyntax $
      emit "INSERT INTO " <> fromMysqlTableName tblName <> emit "(" <>
      mysqlSepBy (emit ", ") (map mysqlIdentifier fields) <> emit ")" <>
      fromMysqlInsertValues values

instance IsSql92InsertValuesSyntax MysqlInsertValuesSyntax where
    type Sql92InsertValuesExpressionSyntax MysqlInsertValuesSyntax = MysqlExpressionSyntax
    type Sql92InsertValuesSelectSyntax MysqlInsertValuesSyntax = MysqlSelectSyntax

    insertSqlExpressions es =
        MysqlInsertValuesSyntax $
        emit "VALUES " <>
        mysqlSepBy (emit ", ")
                   (map (\es' -> emit "(" <>
                                 mysqlSepBy (emit ", ")
                                            (fmap fromMysqlExpression es') <>
                                emit ")")
                        es)
    insertFromSql a = MysqlInsertValuesSyntax (fromMysqlSelect a)

instance IsSql92DeleteSyntax MysqlDeleteSyntax where
    type Sql92DeleteExpressionSyntax MysqlDeleteSyntax = MysqlExpressionSyntax
    type Sql92DeleteTableNameSyntax MysqlDeleteSyntax = MysqlTableNameSyntax

    deleteStmt tbl _ where_ =
      MysqlDeleteSyntax $
      emit "DELETE FROM " <> fromMysqlTableName tbl <>
      maybe mempty (\where' -> emit " WHERE " <> fromMysqlExpression where') where_
    deleteSupportsAlias _ = False

instance IsSql92SelectSyntax MysqlSelectSyntax where
    type Sql92SelectSelectTableSyntax MysqlSelectSyntax = MysqlSelectTableSyntax
    type Sql92SelectOrderingSyntax MysqlSelectSyntax = MysqlOrderingSyntax

    selectStmt tbl ordering limit offset =
      MysqlSelectSyntax $
      fromMysqlSelectTable tbl <>
      (case ordering of
         [] -> mempty
         _  -> emit " ORDER BY " <>
               mysqlSepBy (emit ", ") (map fromMysqlOrdering ordering)) <>
      case (limit, offset) of
        (Just limit', Just offset') ->
            emit " LIMIT " <> emit (integerDec offset') <>
            emit ", " <> emit (integerDec limit')
        (Just limit', Nothing) ->
            emit " LIMIT " <> emit (integerDec limit')
        (Nothing, Just offset') ->
            -- TODO figure out a betterlimit
            emit " LIMIT 1000000000 OFFSET " <> emit (integerDec offset')
        _ -> mempty

instance IsSql92SelectTableSyntax MysqlSelectTableSyntax where
    type Sql92SelectTableSelectSyntax MysqlSelectTableSyntax = MysqlSelectSyntax
    type Sql92SelectTableExpressionSyntax MysqlSelectTableSyntax = MysqlExpressionSyntax
    type Sql92SelectTableProjectionSyntax MysqlSelectTableSyntax = MysqlProjectionSyntax
    type Sql92SelectTableFromSyntax MysqlSelectTableSyntax = MysqlFromSyntax
    type Sql92SelectTableGroupingSyntax MysqlSelectTableSyntax = MysqlGroupingSyntax
    type Sql92SelectTableSetQuantifierSyntax MysqlSelectTableSyntax = MysqlSetQuantifierSyntax

    selectTableStmt setQuantifier proj from where_ grouping having =
      MysqlSelectTableSyntax $
      emit "SELECT " <>
      maybe mempty (\sq' -> fromMysqlSetQuantifier sq' <> emit " ") setQuantifier <>
      fromMysqlProjection proj <>
      maybe mempty (emit " FROM " <>) (fmap fromMysqlFrom from) <>
      maybe mempty (emit " WHERE " <>) (fmap fromMysqlExpression where_) <>
      maybe mempty (emit " GROUP BY " <>) (fmap fromMysqlGrouping grouping) <>
      maybe mempty (emit " HAVING " <>) (fmap fromMysqlExpression having)

    unionTables True  = mysqlTblOp "UNION ALL"
    unionTables False = mysqlTblOp "UNION"
    intersectTables _ = mysqlTblOp "INTERSECT"
    exceptTable _ = mysqlTblOp "EXCEPT"

mysqlTblOp :: Builder -> MysqlSelectTableSyntax -> MysqlSelectTableSyntax -> MysqlSelectTableSyntax
mysqlTblOp op a b =
    MysqlSelectTableSyntax (fromMysqlSelectTable a <> emit " " <> emit op <>
                            emit " " <> fromMysqlSelectTable b)

instance IsSql92AggregationSetQuantifierSyntax MysqlSetQuantifierSyntax where
    setQuantifierDistinct = MysqlSetQuantifierSyntax (emit "DISTINCT")
    setQuantifierAll = MysqlSetQuantifierSyntax (emit "ALL")

instance IsSql92GroupingSyntax MysqlGroupingSyntax where
    type Sql92GroupingExpressionSyntax MysqlGroupingSyntax = MysqlExpressionSyntax

    groupByExpressions es =
      MysqlGroupingSyntax $
      mysqlSepBy (emit ", ") (map fromMysqlExpression es)

instance IsSql92FromSyntax MysqlFromSyntax where
    type Sql92FromExpressionSyntax MysqlFromSyntax = MysqlExpressionSyntax
    type Sql92FromTableSourceSyntax MysqlFromSyntax = MysqlTableSourceSyntax

    fromTable tableSrc Nothing = MysqlFromSyntax (fromMysqlTableSource tableSrc)
    fromTable tableSrc (Just (nm, cols)) =
        MysqlFromSyntax $
        fromMysqlTableSource tableSrc <> emit " AS " <> mysqlIdentifier nm <>
        maybe mempty (mysqlParens . mysqlSepBy (emit ",") . fmap mysqlIdentifier) cols

    innerJoin = mysqlJoin "JOIN"

    leftJoin = mysqlJoin "LEFT JOIN"
    rightJoin = mysqlJoin "RIGHT JOIN"

instance IsSql92FromOuterJoinSyntax MysqlFromSyntax where
    outerJoin = mysqlJoin "OUTER JOIN"

mysqlJoin :: Builder -> MysqlFromSyntax -> MysqlFromSyntax
          -> Maybe MysqlExpressionSyntax -> MysqlFromSyntax
mysqlJoin joinType a b (Just e) =
    MysqlFromSyntax (fromMysqlFrom a <> emit " " <> emit joinType <> emit " " <>
                     fromMysqlFrom b <> emit " ON " <> fromMysqlExpression e)
mysqlJoin joinType a b Nothing =
    MysqlFromSyntax (fromMysqlFrom a <> emit " " <> emit joinType <>
                     emit " " <> fromMysqlFrom b)

instance IsSql92TableSourceSyntax MysqlTableSourceSyntax where
    type Sql92TableSourceSelectSyntax MysqlTableSourceSyntax = MysqlSelectSyntax
    type Sql92TableSourceTableNameSyntax MysqlTableSourceSyntax = MysqlTableNameSyntax
    type Sql92TableSourceExpressionSyntax MysqlTableSourceSyntax = MysqlExpressionSyntax

    tableNamed t = MysqlTableSourceSyntax (fromMysqlTableName t)
    tableFromSubSelect s = MysqlTableSourceSyntax (emit "(" <> fromMysqlSelect s <> emit ")")
    tableFromValues vss = MysqlTableSourceSyntax . mysqlParens $
                          mysqlSepBy (emit " UNION ")
                            (map (mappend (emit "SELECT ") . mysqlSepBy (emit ", ") .
                                    map (mysqlParens . fromMysqlExpression)) vss)

instance IsSql92OrderingSyntax MysqlOrderingSyntax where
    type Sql92OrderingExpressionSyntax MysqlOrderingSyntax = MysqlExpressionSyntax

    ascOrdering e = MysqlOrderingSyntax (fromMysqlExpression e <> emit " ASC")
    descOrdering e = MysqlOrderingSyntax (fromMysqlExpression e <> emit " DESC")

instance IsSql92TableNameSyntax MysqlTableNameSyntax where
  tableName Nothing t = MysqlTableNameSyntax $ mysqlIdentifier t
  tableName (Just schema) t = MysqlTableNameSyntax $ mysqlIdentifier schema <> emit "." <> mysqlIdentifier t

instance IsSql92FieldNameSyntax MysqlFieldNameSyntax where
    qualifiedField a b =
      MysqlFieldNameSyntax $
      mysqlIdentifier a <> emit "." <> mysqlIdentifier b
    unqualifiedField b =
      MysqlFieldNameSyntax (mysqlIdentifier b)

-- | Note: MySQL does not allow timezones in date/time types
instance IsSql92DataTypeSyntax MysqlDataTypeSyntax where
  domainType t = MysqlDataTypeSyntax (mysqlIdentifier t) (mysqlIdentifier t)
  charType len cs = MysqlDataTypeSyntax (emit "CHAR(" <> mysqlCharLen len <> emit ")" <> mysqlOptCharSet cs) (emit "CHAR")
  varCharType len cs = MysqlDataTypeSyntax (emit "VARCHAR(" <> mysqlCharLen len <> emit ")" <> mysqlOptCharSet cs) (emit "CHAR")
  nationalCharType len = MysqlDataTypeSyntax (emit "NATIONAL CHAR(" <> mysqlCharLen  len <> emit ")") (emit "CHAR")
  nationalVarCharType len = MysqlDataTypeSyntax (emit "NATIONAL CHAR VARYING(" <> mysqlCharLen len <> emit ")") (emit "CHAR")
  bitType len = MysqlDataTypeSyntax (emit "BIT(" <> mysqlCharLen len <> emit ")") (emit "BINARY")
  varBitType len = MysqlDataTypeSyntax (emit "VARBINARY(" <> mysqlCharLen len <> emit ")") (emit "BINARY")
  numericType prec = MysqlDataTypeSyntax (emit "NUMERIC" <> mysqlNumPrec prec) (emit "DECIMAL" <> mysqlNumPrec prec)
  decimalType prec = MysqlDataTypeSyntax ty ty
    where ty = emit "DECIMAL" <> mysqlNumPrec prec
  intType = MysqlDataTypeSyntax (emit "INT") (emit "INTEGER")
  smallIntType = MysqlDataTypeSyntax (emit "SMALL INT") (emit "INTEGER")
  floatType prec = MysqlDataTypeSyntax (emit "FLOAT" <> maybe mempty (mysqlParens . emit . fromString . show) prec) (emit "DECIMAL")
  doubleType = MysqlDataTypeSyntax (emit "DOUBLE") (emit "DECIMAL")
  realType = MysqlDataTypeSyntax (emit "REAL") (emit "DECIMAL")

  dateType = MysqlDataTypeSyntax ty ty
    where ty = emit "DATE"
  timeType _prec _withTimeZone = MysqlDataTypeSyntax ty ty
    where ty = emit "TIME"
  timestampType _prec _withTimeZone = MysqlDataTypeSyntax (emit "TIMESTAMP") (emit "DATETIME")

instance IsSql92ProjectionSyntax MysqlProjectionSyntax where
    type Sql92ProjectionExpressionSyntax MysqlProjectionSyntax = MysqlExpressionSyntax

    projExprs exprs =
        MysqlProjectionSyntax $
        mysqlSepBy (emit ", ")
                   (map (\(expr, nm) ->
                             fromMysqlExpression expr <>
                             maybe mempty
                                   (\nm' -> emit " AS " <> mysqlIdentifier nm') nm)
                        exprs)

instance IsCustomSqlSyntax MysqlExpressionSyntax where
    newtype CustomSqlSyntax MysqlExpressionSyntax =
        MysqlCustomExpressionSyntax { fromMysqlCustomExpression :: MysqlSyntax }
        deriving (Monoid, Semigroup)
    customExprSyntax = MysqlExpressionSyntax . fromMysqlCustomExpression
    renderSyntax = MysqlCustomExpressionSyntax . mysqlParens . fromMysqlExpression

instance IsString (CustomSqlSyntax MysqlExpressionSyntax) where
    fromString = MysqlCustomExpressionSyntax . emit . fromString

instance IsSql92ExpressionSyntax MysqlExpressionSyntax where
    type Sql92ExpressionValueSyntax MysqlExpressionSyntax = MysqlValueSyntax
    type Sql92ExpressionSelectSyntax MysqlExpressionSyntax = MysqlSelectSyntax
    type Sql92ExpressionFieldNameSyntax MysqlExpressionSyntax = MysqlFieldNameSyntax
    type Sql92ExpressionQuantifierSyntax MysqlExpressionSyntax = MysqlComparisonQuantifierSyntax
    type Sql92ExpressionCastTargetSyntax MysqlExpressionSyntax = MysqlDataTypeSyntax
    type Sql92ExpressionExtractFieldSyntax MysqlExpressionSyntax = MysqlExtractFieldSyntax

    addE = mysqlBinOp "+"; subE = mysqlBinOp "-"
    mulE = mysqlBinOp "*"; divE = mysqlBinOp "/"; modE = mysqlBinOp "%"

    orE = mysqlBinOp "OR"; andE = mysqlBinOp "AND"
    likeE = mysqlBinOp "LIKE"; overlapsE = mysqlBinOp "OVERLAPS"

    eqE = mysqlCompOp "="; neqE = mysqlCompOp "<>"
    ltE = mysqlCompOp "<"; gtE = mysqlCompOp ">"
    leE = mysqlCompOp "<="; geE = mysqlCompOp ">="

    negateE = mysqlUnOp "-"; notE = mysqlUnOp "NOT"

    existsE s = MysqlExpressionSyntax (emit "EXISTS(" <> fromMysqlSelect s <> emit ")")
    uniqueE s = MysqlExpressionSyntax (emit "UNIQUE(" <> fromMysqlSelect s <> emit ")")

    isNotNullE = mysqlPostFix "IS NOT NULL"; isNullE = mysqlPostFix "IS NULL"
    isTrueE = mysqlPostFix "IS TRUE"; isFalseE = mysqlPostFix "IS FALSE"
    isNotTrueE = mysqlPostFix "IS NOT TRUE"; isNotFalseE = mysqlPostFix "IS NOT FALSE"
    isUnknownE = mysqlPostFix "IS UNKNOWN"; isNotUnknownE = mysqlPostFix "IS NOT UNKNOWN"

    betweenE a b c =
        MysqlExpressionSyntax (emit "(" <> fromMysqlExpression a <> emit ") BETWEEN (" <>
                               fromMysqlExpression b <> emit ") AND (" <>
                               fromMysqlExpression c <> emit ")")

    valueE e = MysqlExpressionSyntax (fromMysqlValue e)
    rowE vs =
        MysqlExpressionSyntax (emit "(" <> mysqlSepBy (emit ", ") (map fromMysqlExpression vs) <> emit ")")
    fieldE fn = MysqlExpressionSyntax (fromMysqlFieldName fn)
    subqueryE s = MysqlExpressionSyntax (emit "(" <> fromMysqlSelect s <> emit ")")

    positionE needle haystack =
        MysqlExpressionSyntax $
        emit "POSITION((" <> fromMysqlExpression needle <> emit ") IN (" <>
        fromMysqlExpression haystack <> emit "))"

    nullIfE a b =
        MysqlExpressionSyntax $
        emit "NULLIF(" <> fromMysqlExpression a <> emit ", " <>
        fromMysqlExpression b <> emit ")"

    absE a = MysqlExpressionSyntax (emit "ABS(" <> fromMysqlExpression a <> emit ")")
    bitLengthE a = MysqlExpressionSyntax (emit "BIT_LENGTH(" <> fromMysqlExpression a <> emit ")")
    charLengthE a = MysqlExpressionSyntax (emit "CHAR_LENGTH(" <> fromMysqlExpression a <> emit ")")
    octetLengthE a = MysqlExpressionSyntax (emit "OCTET_LENGTH(" <> fromMysqlExpression a <> emit ")")
    coalesceE es = MysqlExpressionSyntax (emit "COALESCE(" <>
                                          mysqlSepBy (emit ", ")
                                              (map fromMysqlExpression es) <>
                                          emit ")")
    extractE field from = MysqlExpressionSyntax (emit "EXTRACT(" <> fromMysqlExtractField field <>
                                                 emit " FROM (" <> fromMysqlExpression from <> emit ")")
    castE e to = MysqlExpressionSyntax (emit "CAST((" <> fromMysqlExpression e <> emit ") AS " <>
                                        fromMysqlDataTypeCast to <> emit ")")
    caseE cases else' =
        MysqlExpressionSyntax $
        emit "CASE " <>
        foldMap (\(cond, res) -> emit "WHEN " <> fromMysqlExpression cond <>
                                 emit " THEN " <> fromMysqlExpression res <>
                                 emit " ") cases <>
        emit "ELSE " <> fromMysqlExpression else' <> emit " END"

    currentTimestampE = MysqlExpressionSyntax (emit "CURRENT_TIMESTAMP")
    defaultE = MysqlExpressionSyntax (emit "DEFAULT")

    inE e es = MysqlExpressionSyntax $
               emit "(" <> fromMysqlExpression e <> emit ") IN ( " <>
               mysqlSepBy (emit ", ") (map fromMysqlExpression es) <> emit ")"

    trimE x = MysqlExpressionSyntax (emit "TRIM(" <> fromMysqlExpression x <> emit ")")
    lowerE x = MysqlExpressionSyntax (emit "LOWER(" <> fromMysqlExpression x <> emit ")")
    upperE x = MysqlExpressionSyntax (emit "UPPER(" <> fromMysqlExpression x <> emit ")")

instance IsSql92ExtractFieldSyntax MysqlExtractFieldSyntax where
  secondsField = MysqlExtractFieldSyntax (emit "SECOND")
  minutesField = MysqlExtractFieldSyntax (emit "MINUTE")
  hourField    = MysqlExtractFieldSyntax (emit "HOUR")
  dayField     = MysqlExtractFieldSyntax (emit "DAY")
  monthField   = MysqlExtractFieldSyntax (emit "MONTH")
  yearField    = MysqlExtractFieldSyntax (emit "YEAR")

instance IsSql99ConcatExpressionSyntax MysqlExpressionSyntax where
    concatE [] = valueE (sqlValueSyntax ("" :: T.Text))
    concatE xs =
        MysqlExpressionSyntax . mconcat $
        [ emit "CONCAT("
        , mysqlSepBy (emit ", ") (map fromMysqlExpression xs)
        , emit ")" ]

mysqlUnOp :: Builder -> MysqlExpressionSyntax -> MysqlExpressionSyntax
mysqlUnOp op e = MysqlExpressionSyntax (emit op <> emit " (" <>
                                        fromMysqlExpression e <> emit ")")

mysqlPostFix :: Builder -> MysqlExpressionSyntax -> MysqlExpressionSyntax
mysqlPostFix op e = MysqlExpressionSyntax (emit "(" <> fromMysqlExpression e <>
                                           emit ") " <> emit op)

mysqlCompOp :: Builder -> Maybe MysqlComparisonQuantifierSyntax
            -> MysqlExpressionSyntax -> MysqlExpressionSyntax
            -> MysqlExpressionSyntax
mysqlCompOp op quantifier a b =
    MysqlExpressionSyntax $
    emit "(" <> fromMysqlExpression a <>
    emit ") " <> emit op <>
    maybe mempty (\q -> emit " " <> fromMysqlComparisonQuantifier q <> emit " ") quantifier <>
    emit " (" <> fromMysqlExpression b <> emit ")"

mysqlBinOp :: Builder -> MysqlExpressionSyntax
           -> MysqlExpressionSyntax -> MysqlExpressionSyntax
mysqlBinOp op a b =
    MysqlExpressionSyntax $
    emit "(" <> fromMysqlExpression a <> emit ") " <> emit op <>
    emit " (" <> fromMysqlExpression b <> emit ")"

instance IsSql92AggregationExpressionSyntax MysqlExpressionSyntax where
    type Sql92AggregationSetQuantifierSyntax MysqlExpressionSyntax = MysqlSetQuantifierSyntax

    countAllE = MysqlExpressionSyntax (emit "COUNT(*)")
    countE = mysqlUnAgg "COUNT"
    avgE = mysqlUnAgg "AVG"
    sumE = mysqlUnAgg "SUM"
    minE = mysqlUnAgg "MIN"
    maxE = mysqlUnAgg "MAX"

mysqlUnAgg :: Builder -> Maybe MysqlSetQuantifierSyntax
           -> MysqlExpressionSyntax -> MysqlExpressionSyntax
mysqlUnAgg fn q e =
    MysqlExpressionSyntax $
    emit fn <> emit "(" <>
    maybe mempty (\q' -> fromMysqlSetQuantifier q' <> emit " ") q <>
    fromMysqlExpression e <> emit")"

-- Remove this dependence on Sql99ExpressionSyntax

-- instance IsSql2003EnhancedNumericFunctionsExpressionSyntax MysqlExpressionSyntax where
--     lnE    x = MysqlExpressionSyntax (emit "LN("    <> fromMysqlExpression x <> emit ")")
--     expE   x = MysqlExpressionSyntax (emit "EXP("   <> fromMysqlExpression x <> emit ")")
--     sqrtE  x = MysqlExpressionSyntax (emit "SQRT("  <> fromMysqlExpression x <> emit ")")
--     ceilE  x = MysqlExpressionSyntax (emit "CEIL("  <> fromMysqlExpression x <> emit ")")
--     floorE x = MysqlExpressionSyntax (emit "FLOOR(" <> fromMysqlExpression x <> emit ")")
--     powerE x y = MysqlExpressionSyntax (emit "POWER(" <> fromMysqlExpression x <> emit ", " <>
--                                         fromMysqlExpression y <> emit ")")

instance IsSql92QuantifierSyntax MysqlComparisonQuantifierSyntax where
    quantifyOverAll = MysqlComparisonQuantifierSyntax (emit "ALL")
    quantifyOverAny = MysqlComparisonQuantifierSyntax (emit "ANY")

instance HasSqlValueSyntax MysqlValueSyntax SqlNull where
    sqlValueSyntax _ = MysqlValueSyntax $ emit "NULL"
instance HasSqlValueSyntax MysqlValueSyntax Bool where
    sqlValueSyntax True  = MysqlValueSyntax $ emit "TRUE"
    sqlValueSyntax False = MysqlValueSyntax $ emit "FALSE"

instance HasSqlValueSyntax MysqlValueSyntax Double where
    sqlValueSyntax d = MysqlValueSyntax $ emit (doubleDec d)
instance HasSqlValueSyntax MysqlValueSyntax Float where
    sqlValueSyntax d = MysqlValueSyntax $ emit (floatDec d)

instance HasSqlValueSyntax MysqlValueSyntax Int where
    sqlValueSyntax d = MysqlValueSyntax $ emit (intDec d)
instance HasSqlValueSyntax MysqlValueSyntax Int8 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (int8Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Int16 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (int16Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Int32 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (int32Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Int64 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (int64Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Integer where
    sqlValueSyntax d = MysqlValueSyntax $ emit (integerDec d)

instance HasSqlValueSyntax MysqlValueSyntax Word where
    sqlValueSyntax d = MysqlValueSyntax $ emit (wordDec d)
instance HasSqlValueSyntax MysqlValueSyntax Word8 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (word8Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Word16 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (word16Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Word32 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (word32Dec d)
instance HasSqlValueSyntax MysqlValueSyntax Word64 where
    sqlValueSyntax d = MysqlValueSyntax $ emit (word64Dec d)

instance HasSqlValueSyntax MysqlValueSyntax T.Text where
    sqlValueSyntax t =
        MysqlValueSyntax $ MysqlSyntax
        (\next doEscape before conn ->
             do escaped <- doEscape (TE.encodeUtf8 t)
                next doEscape (before <> "'" <> byteString escaped <> "'") conn)
instance HasSqlValueSyntax MysqlValueSyntax TL.Text where
    sqlValueSyntax = sqlValueSyntax . TL.toStrict
instance HasSqlValueSyntax MysqlValueSyntax [Char] where
    sqlValueSyntax = sqlValueSyntax . T.pack

instance HasSqlValueSyntax MysqlValueSyntax ByteString where
    sqlValueSyntax t =
        MysqlValueSyntax $ MysqlSyntax
        (\next doEscape before conn ->
             do escaped <- doEscape t
                next doEscape (before <> "'" <> byteString escaped <> "'") conn)

instance HasSqlValueSyntax MysqlValueSyntax Scientific where
    sqlValueSyntax = MysqlValueSyntax . emit . scientificBuilder

instance HasSqlValueSyntax MysqlValueSyntax Day where
    sqlValueSyntax d = MysqlValueSyntax (emit ("'" <> dayBuilder d <> "'"))

instance HasSqlValueSyntax MysqlValueSyntax TimeOfDay where
    sqlValueSyntax d = MysqlValueSyntax (emit ("'" <> todBuilder d <> "'"))

dayBuilder :: Day -> Builder
dayBuilder d =
    integerDec year <> "-" <>
    (if month < 10 then "0" else mempty) <> intDec month <> "-" <>
    (if day   < 10 then "0" else mempty) <> intDec day
  where
    (year, month, day) = toGregorian d

todBuilder :: TimeOfDay -> Builder
todBuilder d =
    (if todHour d < 10 then "0" else mempty) <> intDec (todHour d) <> ":" <>
    (if todMin  d < 10 then "0" else mempty) <> intDec (todMin  d) <> ":" <>
    (if secs6 < 10 then "0" else mempty) <> fromString (showFixed False secs6)
  where
    secs6 :: Fixed E6
    secs6 = fromRational (toRational (todSec d))

instance HasSqlValueSyntax MysqlValueSyntax NominalDiffTime where
    sqlValueSyntax d =
        let dWhole = abs (floor d) :: Int
            hours   = dWhole `div` 3600 :: Int

            d' = dWhole - (hours * 3600)
            minutes = d' `div` 60

            seconds = abs d - fromIntegral ((hours * 3600) + (minutes * 60))

            secondsFixed :: Fixed E12
            secondsFixed = fromRational (toRational seconds)
        in
          MysqlValueSyntax $
          emit ((if d < 0 then "-" else mempty) <>
                (if hours < 10 then "0" else mempty) <> intDec hours <> ":" <>
                (if minutes < 10 then "0" else mempty) <> intDec minutes <> ":" <>
                (if secondsFixed < 10 then "0" else mempty) <> fromString (showFixed False secondsFixed))

instance HasSqlValueSyntax MysqlValueSyntax LocalTime where
    sqlValueSyntax d = MysqlValueSyntax (emit ("'" <> dayBuilder (localDay d) <>
                                               " " <> todBuilder (localTimeOfDay d) <> "'"))

instance HasSqlValueSyntax MysqlValueSyntax x => HasSqlValueSyntax MysqlValueSyntax (Maybe x) where
    sqlValueSyntax Nothing = sqlValueSyntax SqlNull
    sqlValueSyntax (Just x) = sqlValueSyntax x

instance HasSqlValueSyntax MysqlValueSyntax A.Value where
    sqlValueSyntax = MysqlValueSyntax . (\x -> emit "'" <> x <> emit "'") . escape . BL.toStrict . A.encode

mysqlCharLen :: Maybe Word -> MysqlSyntax
mysqlCharLen = maybe (emit "MAX") (emit . fromString . show)

mysqlNumPrec :: Maybe (Word, Maybe Word) -> MysqlSyntax
mysqlNumPrec Nothing = mempty
mysqlNumPrec (Just (d, Nothing)) = mysqlParens (emit . fromString . show $ d)
mysqlNumPrec (Just (d, Just n)) = mysqlParens (emit (fromString (show d)) <> emit ", " <> emit (fromString (show n)))

mysqlOptCharSet :: Maybe T.Text -> MysqlSyntax
mysqlOptCharSet Nothing = mempty
mysqlOptCharSet (Just cs) = emit " CHARACTER SET " <> mysqlIdentifier cs
