
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
from datetime import date
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.columns_inputs as f
import exampleenginepythonqiyhbwvw.common.columns_outputs as o
import pyspark.sql.functions as func

from pyspark.sql.types import DecimalType, DateType


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution
    """

    def __init__(self):
        """
        Constructor
        """

        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame :
        self.__logger.info("Applying filter by age and vip status")
        return df.filter((f.edad() >= c.THIRTY_NUMBER) & (f.edad() <= c.FIFTY_NUMBER)\
                         & (f.vip() == c.TRUE_VALUE))

    def join_tables(self,clients_df: DataFrame,contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process")
        full_join = clients_df.join(contracts_df, f.cod_cliente() == f.cod_titular(),c.INNER_TYPE)\
            .join(products_df, [c.COD_PRODUCTO], c.INNER_TYPE)
        return full_join

    def filter_by_number_of_contracts(self, df: DataFrame):
        self.__logger.info("Filtering by number of contracts")
        filter_number = df.select(*df.columns,\
                                  func.count(c.COD_CLIENTE).over(Window.partitionBy(c.COD_CLIENTE))\
                                  .alias(c.COUNT_COLUMN)).filter(func.col(c.COUNT_COLUMN) > 3)\
            .drop(c.COUNT_COLUMN)
        return filter_number

    def hash_columns(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df.select(*df.columns,func.sha2(func.concat_ws(c.CONCAT_SEPARATOR,*df.columns),\
                                            c.SHA_KEY).alias(c.HASH))

    def filter_phones(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by date and brand")
        brands = c.ARRAY_BRAND_PHONE
        country = c.ARRAY_COUNTRY
        filter_df = df.filter((~f.brand().isin(brands)) &\
                              (~f.contry.isin(country)) & \
                              (f.cutoff_date.between("2020-03-01","2020-03-04")))
        return filter_df

    def filter_customers(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by date and card")
        filter_customers_df = df.filter((func.length(f.credit_card_num) <= c.SEVENTEEN) & \
                                        (f.gl_date.between("2020-03-01","2020-03-04")))
        return filter_customers_df

    def join_customers_phone(self,phones_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        self.__logger.info("Joining Phones whit Customers")
        return phones_df.join(customers_df,["customer_id","delivery_id"], c.INNER_TYPE)

    def customer_vip(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column")
        filter_number = df.select(*df.columns, \
                                  func.when((f.prime == c.YES_VALUE) \
                                         & (f.price >= 7500.00),c.YES_VALUE)\
                                  .otherwise(c.NO_VALUE) \
                                  .alias(c.CUSTOMER))
        return filter_number

    def discount(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Add extra_discount column")
        brands = c.ARRAY_BRAND_DISCOUNT
        price = f.price
        discount_df = df.select(*df.columns,\
                                func.when((f.stock < 35) & \
                                       (~f.brand.isin(brands)) & \
                                       (f.prime == c.YES_VALUE),\
                                       (price * .10)
                                       )\
                                .otherwise(0.0).alias(c.EXTRA_DISC)
                                )

        return discount_df

    def final_prices(self,df:DataFrame) -> DataFrame:
        self.__logger.info("Add final price column")
        price = df.select(*df.columns,((f.price + f.taxes) -\
                                       (f.discount + f.extra_discount))\
                          .alias(c.FINAL_PRICE))
        rank = price.select(*price.columns,func.rank()\
                            .over(Window.partitionBy(c.BRAND)
                                  .orderBy(c.FINAL_PRICE)).alias(c.BRANDS_TOP))\
            .filter(f.brands_top <= 50).na.fill("No",["nfc"])
        return rank

    def paste_date(self,df: DataFrame, jwk_date: str) -> DataFrame :
        self.__logger.info("Adding column jwk_date")
        return df.select(*df.columns,func.lit(jwk_date).alias(c.JWK_DATE))

    def age(self,df:DataFrame) -> DataFrame:
        self.__logger.info("Add column birth_date")
        return df.select(*df.columns,\
                         (func.lit(date.today().year) - func.year(f.bird) )\
                         .alias(c.AGE))

    def select_schema(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Seleccting the columns")
        return df.select(o.city_name,
                        o.street_name,
                        o.credit_card_num,
                        o.last_name,
                        o.first_name,
                        o.age,
                        o.brand,
                        o.model,
                        o.nfc,
                        o.contry,
                        o.prime,
                        o.customer_vip,
                        o.taxes.cast(DecimalType(9,2)),
                        o.price,
                        o.discount.cast(DecimalType(9,2)),
                        o.extra_discount.cast(DecimalType(9,2)),
                        o.final_price.cast(DecimalType(9,2)),
                        o.brands_top,
                        o.jwk_date.cast(DateType()))
