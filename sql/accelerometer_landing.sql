CREATE EXTERNAL TABLE IF NOT EXISTS `yared`.`accelerometer_landing` (
    `user` string,
    `timestamp` bigint,
    `x` double,
    `y` double,
    `z` double
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://yared-gomez/accelerometer/landing/';