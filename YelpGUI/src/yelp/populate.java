/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Clob;
import java.text.ParseException;
import org.json.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * @author chris
 */
public class populate {
    
    public static int BUSINESS_DEBUG = 10000;
    public static int USER_DEBUG = 30000;
    public static int REVIEW_DEBUG = 50000;
    
    // DB information
    private static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";
    private static final String USERNAME = "system";
    private static final String PASSWORD = "oracle";
    
    // YelpDataset files
    private static final String USERS_FILE = "./YelpDataset/yelp_user.json"; 
    private static final String REVIEWS_FILE = "./YelpDataset/yelp_review.json";
    private static final String BUSINESSES_FILE = "./YelpDataset/yelp_business.json"; 
    
    // Main category list
    private static final String[] main_category_list = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores",
                    "Dentists", "Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical",
                    "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening",
                    "Nightlife", "Restaurants", "Shopping", "Transportation"
                    };
    private static Set<String> main_categories = new HashSet<String>(); // will initialize once during population of database
    
    public static void populate_users(Connection connection) throws FileNotFoundException, IOException, SQLException, ParseException {
        BufferedReader reader = new BufferedReader(new FileReader(USERS_FILE));
        String line;
        // Prepared Statement which will be reused for each user 
        PreparedStatement statement = connection.prepareStatement("INSERT INTO Users VALUES(?, ?, ?)");
        
        while((line = reader.readLine()) != null) {
            JSONObject obj = new JSONObject(line);
            String yelping_since = obj.getString("yelping_since");
            statement.setString(1, obj.getString("user_id"));
            statement.setString(2, obj.getString("name"));
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
            Date date = format.parse(yelping_since);
            statement.setDate(3, new java.sql.Date(date.getTime()));
            
            statement.executeUpdate();
        }
        statement.close();
        reader.close();  
    }
    
    public static void populate_reviews(Connection connection) throws FileNotFoundException, SQLException, IOException, ParseException {
        BufferedReader reader = new BufferedReader(new FileReader(REVIEWS_FILE));
        String line;
        // Prepared Statement which will be reused for each review 
        PreparedStatement statement = connection.prepareStatement("INSERT INTO Reviews VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        while((line = reader.readLine()) != null) {
            JSONObject obj = new JSONObject(line);
            String review_id = obj.getString("review_id");
            String business_id = obj.getString("business_id");
            String author = obj.getString("user_id");
            String text = obj.getString("text");
            int stars = obj.getInt("stars");
            String publish_date = obj.getString("date");
            JSONObject votes = obj.getJSONObject("votes");
            int funny_votes = votes.getInt("funny");
            int useful_votes = votes.getInt("useful");
            int cool_votes = votes.getInt("cool");
            
            statement.setString(1, review_id);
            statement.setString(2, business_id);
            statement.setString(3, author);
            statement.setString(4, text);
            statement.setInt(5, stars);
            
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-DD");
            Date date = format.parse(publish_date);
            statement.setDate(6, new java.sql.Date(date.getTime()));
            
            statement.setInt(7, funny_votes);
            statement.setInt(8, useful_votes);
            statement.setInt(9, cool_votes);
            
            statement.executeUpdate();
            
        }
        statement.close();
        reader.close();
    }
    
    public static void populate_businesses(Connection connection) throws FileNotFoundException, SQLException, IOException, ParseException {
        // Turn off auto-commit to allow for multi-statement transactions.
        // All insert statements for a single business will represent one transaction.
        // There are multiple tables that store business information
        connection.setAutoCommit(false);
        
        BufferedReader reader = new BufferedReader(new FileReader(BUSINESSES_FILE));
        String line;
        
        // Prepared Statement for inserting into Businesses table
        PreparedStatement business_statement = connection.prepareStatement("INSERT INTO Businesses VALUES(?, ?, ?, ?, ?, ?)");
        // Prepared Statement for inserting into Days_Open table
        PreparedStatement days_open_statement = connection.prepareStatement("INSERT INTO Days_Open VALUES(?, ?, ?, ?)");
        // Prepared Statement for inserting into Main_Categories table
        PreparedStatement main_cat_statement = connection.prepareStatement("INSERT INTO Main_Categories VALUES(?, ?)");
        // Prepared Statement for inserting into Sub_Categories Open table
        PreparedStatement sub_cat_statement = connection.prepareStatement("INSERT INTO Sub_Categories VALUES(?, ?)");
        // Prepared Statement for inserting into Business_Attributes table
        PreparedStatement attr_statement = connection.prepareStatement("INSERT INTO Business_Attributes VALUES(?, ?)");
        
        // Initialize main categories HashSet
        for (String main_category : main_category_list) {
            main_categories.add(main_category);
        }
        
        while((line = reader.readLine()) != null) {
            
            JSONObject obj = new JSONObject(line);

            // Get information about new business
            business_statement.setString(1, obj.getString("business_id"));
            business_statement.setString(2, obj.getString("name"));
            business_statement.setString(3, obj.getString("city"));
            business_statement.setString(4, obj.getString("state"));
            business_statement.setInt(5, obj.getInt("review_count"));
            business_statement.setFloat(6, obj.getFloat("stars"));
            
            
            try {
                // Insert a new business
                business_statement.executeUpdate();
                
                // Insert categories for the business
                insert_business_categories(obj, main_cat_statement, sub_cat_statement);
                
                // Insert attributes for the business
                insert_business_attributes(obj, attr_statement);
                
                // Insert days open/hours for the business
                insert_business_days_open(obj, days_open_statement);
                
            } catch (Exception e) {
                System.out.println("Error while populating db with business_id: " + obj.getString("business_id") + " -- " + e);
                try {
                    connection.rollback();
                } catch (SQLException sqle){
                    System.out.println("Error while rolling back changes: " + sqle);
                }
            } finally {
                try {
                    connection.commit();
                } catch (SQLException sqle){
                    System.out.println("Error while commiting transaction: " + sqle);
                }
            }
    
        }
        business_statement.close();
        days_open_statement.close();
        main_cat_statement.close();
        sub_cat_statement.close();
        attr_statement.close();
        reader.close();
        
        // Set auto commit back to true
        connection.setAutoCommit(true);
    }
    
    public static void insert_business_categories(JSONObject obj, PreparedStatement main_cat_statement, PreparedStatement sub_cat_statement) throws SQLException {
        String business_id = obj.getString("business_id");
        JSONArray categories = obj.getJSONArray("categories");
        List<String> _main_categories = new ArrayList<String>();
        List<String> _sub_categories = new ArrayList<String>();
        String category = null;
        
        // Add business categories to either main/subcategory lists
        for (int i = 0; i < categories.length(); i++){
            category = categories.getString(i);
            if (main_categories.contains(category))
                _main_categories.add(category);
            else
                _sub_categories.add(category);
        }
        
        // Insert main categories into Main_Categories table
        for(int i = 0; i < _main_categories.size(); i++){
            main_cat_statement.setString(1, business_id);
            main_cat_statement.setString(2, _main_categories.get(i));
            main_cat_statement.executeUpdate();
        }
        
        // Insert sub categories into Main_Categories table
        for(int i = 0; i < _sub_categories.size(); i++){
            sub_cat_statement.setString(1, business_id);
            sub_cat_statement.setString(2, _sub_categories.get(i));
            sub_cat_statement.executeUpdate();
        }
    }
    
    public static void insert_business_attributes(JSONObject obj, PreparedStatement attr_statement) throws SQLException{
        List<String> attributes = new ArrayList<String>();
        
        String business_id = obj.getString("business_id");
        JSONObject json_attributes = obj.getJSONObject("attributes");
        Iterator<String> keys = json_attributes.keys();
        
        // Parsee the list of business attributes
        // Assuming only two layers of keys possible (but maybe could be more??)
        while(keys.hasNext()) {
            String key = keys.next();
            if (json_attributes.get(key) instanceof JSONObject) {
                // do something with jsonObject here
                JSONObject sub_jsonObj = (JSONObject) json_attributes.get(key);
                Iterator<String> sub_keys = sub_jsonObj.keys(); 
                while(sub_keys.hasNext()) {
                    String sub_key = (String) sub_keys.next();
                    attributes.add(key + "_" + sub_key + "_" + sub_jsonObj.get(sub_key));
                }
            } else {
                attributes.add(key + "_" + json_attributes.get(key)); // Concatenate key/value
            }
        }
        
        // Insert attributes into Business_Attributes tablwe
        for (int i = 0; i < attributes.size(); i++){
            attr_statement.setString(1, business_id);
            attr_statement.setString(2, attributes.get(i));
            attr_statement.executeUpdate();
        }
        
    }
    
    public static void insert_business_days_open(JSONObject obj, PreparedStatement days_open_statement) throws SQLException, ParseException {
        String business_id = obj.getString("business_id");
        JSONObject hours_obj = obj.getJSONObject("hours");
        Iterator<String> keys = hours_obj.keys();
        
        while(keys.hasNext()) {
            String day = keys.next();
            JSONObject hours = hours_obj.getJSONObject(day);
            String open = hours.getString("open");
            String close = hours.getString("close");
            
            // Set up statement
            days_open_statement.setString(1, business_id);
            days_open_statement.setString(2, day);
                 
            SimpleDateFormat format = new SimpleDateFormat("HH:mm");
            Date open_date = format.parse(open);
            Date close_date = format.parse(close);

            days_open_statement.setDate(3, new java.sql.Date(open_date.getTime()));
            days_open_statement.setDate(4, new java.sql.Date(close_date.getTime()));
            
            days_open_statement.executeUpdate();
        }
        
    }
    
    public static void main(String[] args) {

        // Load the JDBC Driver
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Error loading driver: " + e);
            System.exit(-1);
        }
        
        // Connect to db
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
        } catch (SQLException e){
            System.out.println("Error while connecting to db: " + e);
            System.exit(-1);
        }
        
        try {
            System.out.println("Populating Businesses table. . . ");
            populate_businesses(connection);
            
            System.out.println("Populating Users table. . . ");
            populate_users(connection);
            
            System.out.println("Populating Reviews table. . . ");
            populate_reviews(connection);
            
        } catch (Exception e){
            System.out.println("Error while populating db: " + e);
        }
        
        // Close db connection
        try {
            connection.close();
        } catch (SQLException e){
            System.out.println("Error while closing db connection: " + e);
            System.exit(-1);
        }
       
    }
}
