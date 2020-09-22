-- Extend DB storage size
ALTER DATABASE DATAFILE '/u01/app/oracle/oradata/XE/system.dbf'
AUTOEXTEND ON NEXT 1M MAXSIZE 5000M;

CREATE TABLE Users(
    user_id VARCHAR(30) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    yelping_since DATE NOT NULL
);

CREATE TABLE Businesses(
    business_id VARCHAR(30) PRIMARY KEY,
    business_name VARCHAR(75),
    city VARCHAR(30),
    state VARCHAR(5),
    review_count INTEGER,
    stars NUMBER
);

CREATE TABLE Business_Attributes(
    business_id VARCHAR(30),
    attribute VARCHAR(50),
    PRIMARY KEY(business_id, attribute),
    FOREIGN KEY(business_id) REFERENCES Businesses(business_id) ON DELETE CASCADE
);

CREATE TABLE Main_Categories(
    business_id VARCHAR(30),
    category_name VARCHAR(50),
    PRIMARY KEY(business_id, category_name),
    FOREIGN KEY(business_id) REFERENCES Businesses(business_id) ON DELETE CASCADE
);


CREATE TABLE Sub_Categories(
    business_id VARCHAR(30),
    category_name VARCHAR(50),
    PRIMARY KEY(business_id, category_name),
    FOREIGN KEY(business_id) REFERENCES Businesses(business_id) ON DELETE CASCADE
);

CREATE TABLE Days_Open(
    business_id VARCHAR(30),
    day VARCHAR(20),
    open DATE NOT NULL,
    close DATE NOT NULL,
    PRIMARY KEY(business_id, day),
    FOREIGN KEY(business_id) REFERENCES Businesses(business_id) ON DELETE CASCADE
);

CREATE TABLE Reviews(
    review_id VARCHAR(30) PRIMARY KEY,
    business_id VARCHAR(30) NOT NULL,
    author VARCHAR(30) NOT NULL,
    text CLOB,
    stars INTEGER NOT NULL,
    publish_date DATE NOT NULL,
    useful_votes INTEGER NOT NULL,
    funny_votes INTEGER NOT NULL,
    cool_votes INTEGER NOT NULL,
    FOREIGN KEY(business_id) REFERENCES Businesses(business_id) ON DELETE CASCADE,
    FOREIGN KEY(author) REFERENCES Users(user_id) ON DELETE CASCADE
);
