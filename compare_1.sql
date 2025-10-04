-- ------------------------------
CREATE Procedure [dbo].[incrementClientStockBuy_20130513]
(
  @Date		datetime,
  @ClientID	nvarchar
  (
    12
  )
  ,
  @StockID	nvarchar
  (
    12
  )
  ,
  @Price		float,
  @Volume	numeric,
  @UserID	nvarchar
  (
    30
  )
)
As
declare @buyCommissionPercent float
select @buyCommissionPercent = case when stockvaluation = 0 then 0 else buyCommissionPercent
end
from [client]
where clientid = @clientid
UPDATE [Client Stock]
SET [BuyRT] = [BuyRT] + @Volume,
[UserID] = @UserID ,
[avgprice]= case when
(
  balance + buy + buyRT - sell- sellRT +@volume
)
= 0 then 0 else
(
  (
    case when avgPrice=0 then avgPriceOld else avgPrice
    end *
    (
      balance + buy + buyRT - sell- sellRT
    )
  )
  +
  (
    @volume * @price
  )
  +
  (
    (
      @volume * @price
    )
    * @buycommissionpercent / 100
  )
)
/
(
  balance + buy + buyRT - sell- sellRT +@volume
)
end
WHERE [Date] = @Date AND [ClientID] = @ClientID AND [StockID] = @StockID
IF  @@RowCount <> 1
BEGIN
EXECUTE addClientStock  @Date,
@ClientID,
@StockID,
@UserID
UPDATE [Client Stock]
SET [BuyRT] = [BuyRT] + @Volume,
[UserID] = @UserID ,
[avgprice]= case when
(
  balance + buy + buyRT - sell- sellRT +@volume
)
= 0 then 0 else
(
  (
    case when avgPrice=0 then avgPriceOld else avgPrice
    end *
    (
      balance + buy + buyRT - sell- sellRT
    )
  )
  +
  (
    @volume * @price
  )
  +
  (
    (
      @volume * @price
    )
    * @buycommissionpercent / 100
  )
)
/
(
  balance + buy + buyRT - sell- sellRT +@volume
)
end
WHERE [Date] = @Date AND [ClientID] = @ClientID AND [StockID] = @StockID
END
return


