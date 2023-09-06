CREATE EXTERNAL TABLE `landing_customer`(
  `customername` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `phone` string COMMENT 'from deserializer',
  `birthday` date COMMENT 'from deserializer',
  `registrationdate` date COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `lastupdatedate` bigint COMMENT 'from deserializer',
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer',
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/customer/'
TBLPROPERTIES (
  'TableType'='EXTERNAL_TABLE',
  'classification'='json')