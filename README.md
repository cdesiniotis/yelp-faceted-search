# Yelp Faceted Search
A data analysis application for performing faceted search on Yelp's business review data.

Here is a demo for searching delis in Las Vegas:
[![Demo](https://j.gifs.com/5QPBQR.gif)](https://youtu.be/kIAFm9AJ-n8)

## Overview
This project is a standalone Java application for running queries on a real Yelp dataset. The dataset used is a subset of the one used in the 2013 "Yelp Dataset Challenge," containing **20,544 businesses**, **211,002 users**, and **826,190 reviews**. Users can search for businesses based on business categories (main & sub-categories), business attributes (e.g. accepts credit cards, has delivery, etc.), location, and days/hours open. Each query returns matched businesses and allows the user to view all the reviews for each business.

The application utilizes *faceted search*, which is a popular technique in commercial search applications. It is a technique for accessing information organized according to a faceted classification system, allowing users to explore a collection of information by applying multiple filters.

**The emphasis for this project is on the database infrastructure**, not on the user interface design. This project was built using NetBeans.

## Run
Extract the Yelp dataset:
```
$ cd YelpGUI
$ tar -xvzf YelpDataset.tar.gz
```
Bring up the Oracle database:
```
$ docker run -d -p 1521:1521 epiclabs/docker-oracle-xe-11g
```
Initialize the database with the necessary tables/indices (default username/pw is system/oracle):
```
$ sqlplus ${user}/${pass} @createdb.sql
$ sqlplus ${user}/${pass} @createIndex.sql
```
Populate the database with the Yelp dataset (**Warning**: this may take about 20 mins):
```
$ javac -cp "./dist/lib/ojdbc.jar:./dist/lib/json-20190722.jar" src/yelp/populate.java
$ java src.yelp.populate
```
Run the GUI:
```
$ java -jar dist/YelpGUI.jar
```
Delete the database:
```
$ sqlplus ${user}/${pass} @dropdb.sql
```
