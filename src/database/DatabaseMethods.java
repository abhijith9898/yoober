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
    statement.close();
    allAccountsResult.close();

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
    PreparedStatement avgRatingByEmailStmt = conn.prepareStatement(avgRatingByEmailQuery);
    avgRatingByEmailStmt.setString(1, driverEmail);
    ResultSet avgRatingByEmailResult = avgRatingByEmailStmt.executeQuery();
    averageRating = avgRatingByEmailResult.getDouble("AVG_RATING");
    // System.out.println("average ratng" + averageRating);
    avgRatingByEmailStmt.close();
    avgRatingByEmailResult.close();
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

    int accountId = insertAccount(account);
    if (accountId != -1) {
      if (account.isDriverAndPassenger()) {
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
    String firstName = account.getFirstName();
    String lastName = account.getLastName();
    Address address = account.getAddress();
    String phoneNumber = account.getPhoneNumber();
    String email = account.getEmail();
    String birthdate = account.getBirthdate();

    int addressId = -1;

    addressId = insertAddressIfNotExists(address);


    if (addressId != -1) {
      String insertAccountQuery = "INSERT INTO accounts (FIRST_NAME,LAST_NAME,BIRTHDATE,ADDRESS_ID,PHONE_NUMBER,EMAIL) VALUES (?,?,?,?,?,?)";
      PreparedStatement insertAccountStmt = conn.prepareStatement(insertAccountQuery,
          Statement.RETURN_GENERATED_KEYS);
      insertAccountStmt.setString(1, firstName);
      insertAccountStmt.setString(2, lastName);
      insertAccountStmt.setString(3, birthdate);
      insertAccountStmt.setInt(4, addressId);
      insertAccountStmt.setString(5, phoneNumber);
      insertAccountStmt.setString(6, email);
      insertAccountStmt.executeUpdate();
      ResultSet keys = insertAccountStmt.getGeneratedKeys();
      keys.next();
      accountId = keys.getInt(1);
      insertAccountStmt.close();
      keys.close();
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

    String creditCardNumber = passenger.getCreditCardNumber();
    String insertPassengerQuery = "INSERT INTO passengers VALUES (?,?);";
    PreparedStatement insertPassengerStmt = conn.prepareStatement(insertPassengerQuery,
        Statement.RETURN_GENERATED_KEYS);
    insertPassengerStmt.setInt(1, accountId);
    insertPassengerStmt.setString(2, creditCardNumber);
    insertPassengerStmt.executeUpdate();
    ResultSet keys = insertPassengerStmt.getGeneratedKeys();
    keys.next();
    accountId = keys.getInt(1);
    insertPassengerStmt.close();
    keys.close();
    return accountId;
  }

  /*
   * Accepts: Driver details (should not be null), and account id for the driver
   * Behaviour: Inserts the new driver and driver's license record, correctly
   * linked to the account id
   * Returns: Id of the new driver
   */
  public int insertDriver(Driver driver, int accountId) throws SQLException {

    int licenseId = -1;
    String licenseNumber = driver.getLicenseNumber();
    String licenseExpiry = driver.getLicenseExpiryDate();
    licenseId = insertLicense(licenseNumber, licenseExpiry);
    if (licenseId != -1) {
      String insertDriverQuery = "INSERT INTO drivers values (?,?)";
      PreparedStatement insertDriverStmt = conn.prepareStatement(insertDriverQuery, Statement.RETURN_GENERATED_KEYS);
      insertDriverStmt.setInt(1, accountId);
      insertDriverStmt.setInt(2, licenseId);
      insertDriverStmt.executeUpdate();
      ResultSet keys = insertDriverStmt.getGeneratedKeys();
      keys.next();
      accountId = keys.getInt(1);
      insertDriverStmt.close();
      keys.close();
    }
    return accountId;
  }

  /*
   * Accepts: Driver's license number and license expiry
   * Behaviour: Inserts the new driver's license record
   * Returns: Id of the new driver's license
   */
  public int insertLicense(String licenseNumber, String licenseExpiry) throws SQLException {
    int licenseId = -1;
    String insertLicenseQuery = "INSERT INTO licenses (NUMBER,EXPIRY_DATE) VALUES (?,?)";
    PreparedStatement insertLicenseStmt = conn.prepareStatement(insertLicenseQuery, Statement.RETURN_GENERATED_KEYS);
    insertLicenseStmt.setString(1, licenseNumber);
    insertLicenseStmt.setString(2, licenseExpiry);
    insertLicenseStmt.executeUpdate();
    ResultSet keys = insertLicenseStmt.getGeneratedKeys();
    keys.next();
    licenseId = keys.getInt(1);
    insertLicenseStmt.close();
    keys.close();
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

    String checkAddressExistQuery = "SELECT ID FROM addresses WHERE STREET = ? AND CITY= ? AND PROVINCE= ? AND POSTAL_CODE= ?";
    PreparedStatement checkAddressExistStmt = conn.prepareStatement(checkAddressExistQuery);
    checkAddressExistStmt.setString(1, street);
    checkAddressExistStmt.setString(2, city);
    checkAddressExistStmt.setString(3, province);
    checkAddressExistStmt.setString(4, postalCode);
    ResultSet checkAddressExistResult = checkAddressExistStmt.executeQuery();
    if (checkAddressExistResult.next()) {
      addressId = checkAddressExistResult.getInt("ID");
      checkAddressExistResult.close();
      checkAddressExistStmt.close();
    } else {
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
      keys.close();
    }

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
    int passengerId = getPassengerIdFromEmail(passengerEmail);
    String insertFavouriteDestinationQuery = "INSERT INTO favourite_locations (PASSENGER_ID, LOCATION_ID, NAME) VALUES (?,?,?)";
    PreparedStatement insertFavouriteDestinationStmt = conn.prepareStatement(insertFavouriteDestinationQuery);
    insertFavouriteDestinationStmt.setString(3, favouriteName);
    insertFavouriteDestinationStmt.setInt(1, passengerId);
    insertFavouriteDestinationStmt.setInt(2, addressId);
    insertFavouriteDestinationStmt.executeUpdate();
    insertFavouriteDestinationStmt.close();

  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a driver exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkDriverExists(String email) throws SQLException {
    String checkDriverQuery = "SELECT d.ID as DRIVER_ID FROM accounts a INNER JOIN drivers d ON a.ID = d.ID WHERE a.EMAIL = ?";
    PreparedStatement checkDriverStmt = conn.prepareStatement(checkDriverQuery);
    checkDriverStmt.setString(1, email);
    ResultSet checkDriverResult = checkDriverStmt.executeQuery();
    if (checkDriverResult.next()) {
      checkDriverStmt.close();
      checkDriverResult.close();
      return true;
    } else {
      checkDriverStmt.close();
      checkDriverResult.close();
      return false;
    }
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a passenger exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkPassengerExists(String email) throws SQLException {
    String checkPassengerQuery = "SELECT p.ID as PASSENGER_ID FROM accounts a INNER JOIN passengers p ON a.ID = p.ID WHERE a.EMAIL = ?";
    PreparedStatement checkPassengerStmt = conn.prepareStatement(checkPassengerQuery);
    checkPassengerStmt.setString(1, email);
    ResultSet checkPassengerResult = checkPassengerStmt.executeQuery();
    if (checkPassengerResult.next()) {
      checkPassengerStmt.close();
      checkPassengerResult.close();
      return true;
    } else {
      checkPassengerStmt.close();
      checkPassengerResult.close();
      return false;
    }
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
    String insertRideRequestQuery = "INSERT INTO ride_requests (PASSENGER_ID,PICKUP_LOCATION_ID,PICKUP_DATE,PICKUP_TIME,NUMBER_OF_RIDERS,DROPOFF_LOCATION_ID) VALUES (?,?,?,?,?,?)";
    PreparedStatement insertRideRequestStmt = conn.prepareStatement(insertRideRequestQuery);
    insertRideRequestStmt.setInt(1, passengerId);
    insertRideRequestStmt.setInt(2, pickupAddressId);
    insertRideRequestStmt.setString(3, date);
    insertRideRequestStmt.setString(4, time);
    insertRideRequestStmt.setInt(5, numberOfPassengers);
    insertRideRequestStmt.setInt(6, dropoffLocationId);
    insertRideRequestStmt.executeUpdate();
    insertRideRequestStmt.close();
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of passenger with specified email (assumes passenger
   * exists)
   * Returns: Id
   */
  public int getPassengerIdFromEmail(String passengerEmail) throws SQLException {
    int passengerId = -1;
    String getPassengerIdFromEmailQuery = "SELECT a.ID FROM accounts a INNER JOIN passengers p ON a.ID = p.ID WHERE a.EMAIL = ?";
    PreparedStatement getPassengerFromEmailIdStmt = conn.prepareStatement(getPassengerIdFromEmailQuery);
    getPassengerFromEmailIdStmt.setString(1, passengerEmail);
    ResultSet passengerIdResultSet = getPassengerFromEmailIdStmt.executeQuery();
    passengerIdResultSet.next();
    passengerId = passengerIdResultSet.getInt(1);
    getPassengerFromEmailIdStmt.close();
    passengerIdResultSet.close();
    return passengerId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of driver with specified email (assumes driver exists)
   * Returns: Id
   */
  public int getDriverIdFromEmail(String driverEmail) throws SQLException {
    int driverId = -1;
    String getDriverQuery = "SELECT d.ID as DRIVER_ID FROM accounts a INNER JOIN drivers d ON a.ID = d.ID WHERE a.EMAIL = ?";
    PreparedStatement getDriverStmt = conn.prepareStatement(getDriverQuery);
    getDriverStmt.setString(1, driverEmail);
    ResultSet getDriverResult = getDriverStmt.executeQuery();
    driverId = getDriverResult.getInt("DRIVER_ID");
    getDriverStmt.close();
    getDriverResult.close();

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
    String getAccountAddressIdFromEmailQuery = "SELECT a.ADDRESS_ID from accounts a INNER JOIN passengers p ON p.ID = a.ID WHERE a.EMAIL = ?";
    PreparedStatement getAccountAddressIdFromEmailStmt = conn.prepareStatement(getAccountAddressIdFromEmailQuery);
    getAccountAddressIdFromEmailStmt.setString(1, email);
    ResultSet getAccountAddressIdFromEmailResult = getAccountAddressIdFromEmailStmt.executeQuery();
    getAccountAddressIdFromEmailResult.next();
    addressId = getAccountAddressIdFromEmailResult.getInt(1);
    getAccountAddressIdFromEmailStmt.close();
    getAccountAddressIdFromEmailResult.close();
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
    String getFavouriteDestinationForPassengerQuery = "SELECT f.NAME, a.ID, a.STREET, a.CITY, a.PROVINCE, a.POSTAL_CODE FROM favourite_locations f INNER JOIN addresses a ON f.LOCATION_ID = a.ID INNER JOIN passengers p ON p.ID = f.ID INNER JOIN accounts ac ON ac.ID = p.ID WHERE ac.EMAIL = ?";
    PreparedStatement getFavouriteDestinationForPassengerStmt = conn
        .prepareStatement(getFavouriteDestinationForPassengerQuery);
    getFavouriteDestinationForPassengerStmt.setString(1, passengerEmail);
    ResultSet getFavouriteDestinationForPassengerResult = getFavouriteDestinationForPassengerStmt.executeQuery();
    while (getFavouriteDestinationForPassengerResult.next()) {
      String destinationName = getFavouriteDestinationForPassengerResult.getString("NAME");
      String streetName = getFavouriteDestinationForPassengerResult.getString("STREET");
      String cityName = getFavouriteDestinationForPassengerResult.getString("CITY");
      String provinceName = getFavouriteDestinationForPassengerResult.getString("PROVINCE");
      String postalCode = getFavouriteDestinationForPassengerResult.getString("POSTAL_CODE");
      int addressId = getFavouriteDestinationForPassengerResult.getInt("ID");
      FavouriteDestination favouriteDestination = new FavouriteDestination(destinationName, addressId, streetName,
          cityName, provinceName, postalCode);
      favouriteDestinations.add(favouriteDestination);
    }
    getFavouriteDestinationForPassengerStmt.close();
    getFavouriteDestinationForPassengerResult.close();
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

    String uncompleteRequestQuery = "SELECT rr.ID AS REQUEST_ID,FIRST_NAME,LAST_NAME, ad1.STREET AS PICKUP_STREET,ad1.CITY AS PICKUP_CITY, ad2.STREET AS DROP_STREET,ad2.CITY AS DROP_CITY, PICKUP_TIME, PICKUP_DATE FROM ride_requests rr LEFT JOIN rides r ON rr.ID = r.REQUEST_ID INNER JOIN passengers p ON p.ID = rr.PASSENGER_ID INNER JOIN accounts a ON a.ID = p.ID INNER JOIN addresses ad1 ON rr.PICKUP_LOCATION_ID = ad1.ID INNER JOIN addresses ad2 ON rr.DROPOFF_LOCATION_ID = ad2.ID  WHERE r.REQUEST_ID ISNULL";
    Statement statement = conn.createStatement();
    ResultSet uncompleteRequestResult = statement.executeQuery(uncompleteRequestQuery);

    while (uncompleteRequestResult.next()) {
      String firstName = uncompleteRequestResult.getString("FIRST_NAME");
      String lastName = uncompleteRequestResult.getString("LAST_NAME");
      int requestId = uncompleteRequestResult.getInt("REQUEST_ID");
      String pickupStreet = uncompleteRequestResult.getString("PICKUP_STREET");
      String pickupCity = uncompleteRequestResult.getString("PICKUP_CITY");
      String dropStreet = uncompleteRequestResult.getString("DROP_STREET");
      String dropCity = uncompleteRequestResult.getString("DROP_CITY");
      String pickupTime = uncompleteRequestResult.getString("PICKUP_TIME");
      String pickupDate = uncompleteRequestResult.getString("PICKUP_DATE");
      RideRequest obj = new RideRequest(requestId, firstName, lastName, pickupStreet, pickupCity, dropStreet, dropCity,
          pickupDate, pickupTime);
      uncompletedRideRequests.add(obj);
    }
    statement.close();
    uncompleteRequestResult.close();

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
    int driverId = getDriverIdFromEmail(ride.getDriverEmail());
    String completeRideQuery = "INSERT INTO rides (DRIVER_ID,REQUEST_ID,ACTUAL_START_DATE,ACTUAL_START_TIME,ACTUAL_END_DATE,ACTUAL_END_TIME,RATING_FROM_DRIVER,RATING_FROM_PASSENGER,DISTANCE,CHARGE) VALUES (?,?,?,?,?,?,?,?,?,?)";
    PreparedStatement completeRideStmt = conn.prepareStatement(completeRideQuery);
    completeRideStmt.setInt(1, driverId);
    completeRideStmt.setInt(2, ride.getRideRequestId());
    completeRideStmt.setString(3, ride.getStartDate());
    completeRideStmt.setString(4, ride.getStartTime());
    completeRideStmt.setString(5, ride.getEndDate());
    completeRideStmt.setString(6, ride.getEndTime());
    completeRideStmt.setInt(7, ride.getRatingFromDriver());
    completeRideStmt.setInt(8, ride.getRatingFromPassenger());
    completeRideStmt.setDouble(9, ride.getDistance());
    completeRideStmt.setDouble(10, ride.getCharge());
    completeRideStmt.executeUpdate();
    completeRideStmt.close();
  }

}
