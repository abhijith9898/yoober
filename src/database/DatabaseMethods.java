/*
 * Group members: ABHIJITH RAJEEV, ELDHOSE BABU
 * Instructions: For Project 2, implement all methods in this class, and test to confirm they behave as expected when the program is run.
 */

package database;

import java.sql.*;
import java.util.*;

import dataClasses.*;
import dataClasses.Driver;

public class DatabaseMethods {
  private Connection conn;

  public DatabaseMethods(Connection conn) {
    this.conn = conn;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Retrieves information about all accounts
   * Returns: List of account objects
   */
  public ArrayList<Account> getAllAccounts() throws SQLException {
    ArrayList<Account> accounts = new ArrayList<Account>();

    String getAllAccountQuery = "SELECT FIRST_NAME, LAST_NAME, STREET, CITY, PROVINCE, POSTAL_CODE, PHONE_NUMBER, EMAIL, BIRTHDATE, p.ID AS PASSENGER_ID, d.ID AS DRIVER_ID FROM accounts a INNER JOIN addresses ad ON a.ADDRESS_ID = ad.ID LEFT JOIN passengers p ON a.ID=p.ID LEFT JOIN drivers d ON a.ID=d.ID;";
    Statement statement = conn.createStatement();
    ResultSet allAccountsResult = statement.executeQuery(getAllAccountQuery);

    while (allAccountsResult.next()) {
      String firstName = allAccountsResult.getString("FIRST_NAME");
      String lastName = allAccountsResult.getString("LAST_NAME");
      String birthdate = allAccountsResult.getString("BIRTHDATE");
      String street = allAccountsResult.getString("STREET");
      String city = allAccountsResult.getString("CITY");
      String province = allAccountsResult.getString("PROVINCE");
      String postalCode = allAccountsResult.getString("POSTAL_CODE");
      String phoneNumber = allAccountsResult.getString("PHONE_NUMBER");
      String email = allAccountsResult.getString("EMAIL");
      boolean isPassenger = allAccountsResult.getBoolean("PASSENGER_ID");
      boolean isDriver = allAccountsResult.getBoolean("DRIVER_ID");
      Account accountObj = new Account(firstName, lastName, street, city, province, postalCode, phoneNumber, email,
          birthdate, isPassenger, isDriver);
      accounts.add(accountObj);
    }

    return accounts;
  }

  /*
   * Accepts: Email address of driver
   * Behaviour: Calculates the average rating over all rides performed by the
   * driver specified by the email address
   * Returns: The average rating value
   */
  public double getAverageRatingForDriver(String driverEmail) throws SQLException {
    double averageRating = 0.0;
    String avgRatingByEmailQuery = "SELECT AVG(RATING_FROM_PASSENGER) AS AVG_RATING from rides r LEFT JOIN drivers d ON r.DRIVER_ID=d.ID LEFT JOIN accounts a ON d.ID=a.ID WHERE a.EMAIL = ?";
    // Statement statement = conn.createStatement();
    // ResultSet avgRatingByEmailResult =
    // statement.executeQuery(avgRatingByEmailQuery);
    // averageRating = avgRatingByEmailResult.getDouble("AVG_RATING");

    PreparedStatement avgRatingByEmailStmt = conn.prepareStatement(avgRatingByEmailQuery);
    avgRatingByEmailStmt.setString(1, driverEmail);
    ResultSet avgRatingByEmailResult = avgRatingByEmailStmt.executeQuery();
    averageRating = avgRatingByEmailResult.getDouble("AVG_RATING");
    // System.out.println("average ratng" + averageRating);

    return averageRating;
  }

  /*
   * Accepts: Account details, and passenger and driver specific details.
   * Passenger or driver details could be
   * null if account is only intended for one type of use.
   * Behaviour:
   * - Insert new account using information provided in Account object
   * - For non-null passenger/driver details, insert the associated data into the
   * relevant tables
   * Returns: Nothing
   */
  public void createAccount(Account account, Passenger passenger, Driver driver) throws SQLException {
    // TODO: Implement
    // Hint: Use the available insertAccount, insertPassenger, and insertDriver
    // methods
    int accountId = insertAccount(account);
    if (accountId != -1) {
      if (account.isPassenger() && account.isDriver()) {
        insertPassenger(passenger, accountId);
        insertDriver(driver, accountId);
      } else if (account.isDriver()) {
        insertDriver(driver, accountId);
      } else if (account.isPassenger()) {
        insertPassenger(passenger, accountId);
      }
    }
  }

  /*
   * Accepts: Account details (which includes address information)
   * Behaviour: Inserts the new account, as well as the account's address if it
   * doesn't already exist. The new/existing address should
   * be linked to the account
   * Returns: Id of the new account
   */
  public int insertAccount(Account account) throws SQLException {
    int accountId = -1;

    // TODO: Implement
    // Hint: Use the insertAddressIfNotExists method
    String firstName = account.getFirstName();
    String lastName = account.getLastName();
    Address address = account.getAddress();
    String street = account.getStreet();
    String city = account.getCity();
    String province = account.getProvince();
    String postalCode = account.getPostalCode();
    String phoneNumber = account.getPhoneNumber();
    String email = account.getEmail();
    String birthdate = account.getBirthdate();

    String checkAddressExistQuery = "SELECT ID FROM addresses WHERE STREET = ? AND CITY= ? AND PROVINCE= ? AND POSTAL_CODE= ?";
    PreparedStatement checkAddressExistStmt = conn.prepareStatement(checkAddressExistQuery);
    checkAddressExistStmt.setString(1, street);
    checkAddressExistStmt.setString(2, city);
    checkAddressExistStmt.setString(3, province);
    checkAddressExistStmt.setString(4, postalCode);
    ResultSet checkAddressExistResult = checkAddressExistStmt.executeQuery();
    int addressId = -1;
    if(checkAddressExistResult.next()){
      addressId = checkAddressExistResult.getInt("ID");
    } else {
      addressId = insertAddressIfNotExists(address);
    }    
    checkAddressExistStmt.close();
    
    if(addressId != -1){
      String insertAccountQuery = "INSERT INTO accounts (FIRST_NAME,LAST_NAME,BIRTHDATE,ADDRESS_ID,PHONE_NUMBER,EMAIL) VALUES (?,?,?,?,?,?)";
      PreparedStatement insertAccountStmt = conn.prepareStatement(insertAccountQuery, Statement.RETURN_GENERATED_KEYS);
      insertAccountStmt.setString(1, firstName);
      insertAccountStmt.setString(2, lastName);
      insertAccountStmt.setString(3, birthdate);
      insertAccountStmt.setInt(4,addressId);
      insertAccountStmt.setString(5, phoneNumber);
      insertAccountStmt.setString(6, email);
      insertAccountStmt.executeUpdate();
      ResultSet keys = insertAccountStmt.getGeneratedKeys();
      keys.next();
      accountId = keys.getInt(1);
      insertAccountStmt.close();
    }   

    return accountId;
  }

  /*
   * Accepts: Passenger details (should not be null), and account id for the
   * passenger
   * Behaviour: Inserts the new passenger record, correctly linked to the account
   * id
   * Returns: Id of the new passenger
   */
  public int insertPassenger(Passenger passenger, int accountId) throws SQLException {
    // TODO: Implement
    String insertPassengerQuery = "INSERT INTO passengers VALUES (?,?);";
    PreparedStatement insertPassengerStmt = conn.prepareStatement(insertPassengerQuery, Statement.RETURN_GENERATED_KEYS);

    return accountId;
  }

  /*
   * Accepts: Driver details (should not be null), and account id for the driver
   * Behaviour: Inserts the new driver and driver's license record, correctly
   * linked to the account id
   * Returns: Id of the new driver
   */
  public int insertDriver(Driver driver, int accountId) throws SQLException {
    // TODO: Implement
    // Hint: Use the insertLicense method

    return accountId;
  }

  /*
   * Accepts: Driver's license number and license expiry
   * Behaviour: Inserts the new driver's license record
   * Returns: Id of the new driver's license
   */
  public int insertLicense(String licenseNumber, String licenseExpiry) throws SQLException {
    int licenseId = -1;
    // TODO: Implement

    return licenseId;
  }

  /*
   * Accepts: Address details
   * Behaviour:
   * - Checks if an address with these properties already exists.
   * - If it does, gets the id of the existing address.
   * - If it does not exist, creates the address in the database, and gets the id
   * of the new address
   * Returns: Id of the address
   */
  public int insertAddressIfNotExists(Address address) throws SQLException {
    int addressId = -1;
    String street = address.getStreet();
    String city = address.getCity();
    String province = address.getProvince();
    String postalCode = address.getPostalCode();
    // TODO: Implement
    String insertAddressSql = "INSERT INTO addresses (STREET,CITY,PROVINCE,POSTAL_CODE) VALUES (?,?,?,?)";
    PreparedStatement insertAddressStmt = conn.prepareStatement(insertAddressSql, Statement.RETURN_GENERATED_KEYS);
    insertAddressStmt.setString(1, street);
    insertAddressStmt.setString(2, city);
    insertAddressStmt.setString(3, province);
    insertAddressStmt.setString(4, postalCode);
    insertAddressStmt.executeUpdate();
    ResultSet keys = insertAddressStmt.getGeneratedKeys();
    keys.next();
    addressId = keys.getInt(1);
    insertAddressStmt.close();

    return addressId;
  }

  /*
   * Accepts: Name of new favourite destination, email address of the passenger,
   * and the id of the address being favourited
   * Behaviour: Finds the id of the passenger with the email address, then inserts
   * the new favourite destination record
   * Returns: Nothing
   */
  public void insertFavouriteDestination(String favouriteName, String passengerEmail, int addressId)
      throws SQLException {
    // TODO: Implement
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a driver exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkDriverExists(String email) throws SQLException {
    // TODO: Implement

    return true;
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a passenger exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkPassengerExists(String email) throws SQLException {
    // TODO: Implement

    return true;
  }

  /*
   * Accepts: Email address of passenger making request, id of dropoff address,
   * requested date/time of ride, and number of passengers
   * Behaviour: Inserts a new ride request, using the provided properties
   * Returns: Nothing
   */
  public void insertRideRequest(String passengerEmail, int dropoffLocationId, String date, String time,
      int numberOfPassengers) throws SQLException {
    int passengerId = this.getPassengerIdFromEmail(passengerEmail);
    int pickupAddressId = this.getAccountAddressIdFromEmail(passengerEmail);

    // TODO: Implement
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of passenger with specified email (assumes passenger
   * exists)
   * Returns: Id
   */
  public int getPassengerIdFromEmail(String passengerEmail) throws SQLException {
    int passengerId = -1;
    // TODO: Implement

    return passengerId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of driver with specified email (assumes driver exists)
   * Returns: Id
   */
  public int getDriverIdFromEmail(String driverEmail) throws SQLException {
    int driverId = -1;
    // TODO: Implement

    return driverId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets the id of the address tied to the account with the provided
   * email address
   * Returns: Address id
   */
  public int getAccountAddressIdFromEmail(String email) throws SQLException {
    int addressId = -1;
    // TODO: Implement

    return addressId;
  }

  /*
   * Accepts: Email address of passenger
   * Behaviour: Gets a list of all the specified passenger's favourite
   * destinations
   * Returns: List of favourite destinations
   */
  public ArrayList<FavouriteDestination> getFavouriteDestinationsForPassenger(String passengerEmail)
      throws SQLException {
    ArrayList<FavouriteDestination> favouriteDestinations = new ArrayList<FavouriteDestination>();

    // TODO: Implement

    return favouriteDestinations;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Gets a list of all uncompleted ride requests (i.e. requests
   * without an associated ride record)
   * Returns: List of all uncompleted rides
   */
  public ArrayList<RideRequest> getUncompletedRideRequests() throws SQLException {
    ArrayList<RideRequest> uncompletedRideRequests = new ArrayList<RideRequest>();

    // TODO: Implement

    return uncompletedRideRequests;
  }

  /*
   * Accepts: Ride details
   * Behaviour: Inserts a new ride record
   * Returns: Nothing
   */
  public void insertRide(Ride ride) throws SQLException {
    // TODO: Implement
    // Hint: Use getDriverIdFromEmail
  }

}
