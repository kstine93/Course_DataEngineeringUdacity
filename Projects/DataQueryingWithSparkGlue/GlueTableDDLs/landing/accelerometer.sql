CREATE EXTERNAL TABLE `landing_accelerometer`(
  `user` string COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `x` float COMMENT 'from deserializer',
  `y` float COMMENT 'from deserializer',
  `z` float COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/accelerometer/'
TBLPROPERTIES (
  'TableType'='EXTERNAL_TABLE',
  'classification'='json')