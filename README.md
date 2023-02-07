
How do I Ingest Small Files into Hudi Datallake with Glue Incremental data processing  

![image](https://user-images.githubusercontent.com/39345855/217305247-8215d7b6-c763-4c0a-83a0-7dc48cc25d38.png)




### Make sure to add these on Job Paramaters 
![image](https://user-images.githubusercontent.com/39345855/217310224-a27c69c6-f1f2-4dfb-8809-ef6e5353d548.png)
```
--write-shuffle-files-ato-s3   true
--conf  spark.shuffle.storage.path=s3://XXXXX/shuffle

```


# Step 1:
#### Run python file to populate s3 with lot of smaller file 


* Make sure yo add your Access and Secret key in .env files 
```
python run.py
```

# Step 2:
#### Run your Glue Job 


# Refernces 

Introducing the Cloud Shuffle Storage Plugin for Apache Spark
https://aws.amazon.com/blogs/big-data/introducing-the-cloud-shuffle-storage-plugin-for-apache-spark/







