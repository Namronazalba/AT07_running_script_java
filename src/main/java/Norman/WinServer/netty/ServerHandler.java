package Norman.WinServer.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private static final EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(8);
    private final JdbcTemplate jdbcTemplate;
    private final int batchSize = 1;
    private final Queue<String> dataQueue = new LinkedList<>();
    public ServerHandler() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//        dataSource.setUrl("jdbc:mysql://localhost:3306/db_ezpht?useSSL=false");
        dataSource.setUrl("jdbc:mysql://192.168.1.242:3306/eztph_db?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("pass");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        try {
            byte[] buffer = new byte[in.readableBytes()];
            in.readBytes(buffer);
            String receivedData = new String(buffer, StandardCharsets.UTF_8);
            String[] individualDataPoints = receivedData.split("\\r?\\n");
            for (String dataPoint : individualDataPoints) {
                dataQueue.offer(dataPoint);
            }
            if (dataQueue.size() >= batchSize) {
                processBatch(ctx);
            }
        } finally {
            in.release();
        }
    }
    private void processBatch(ChannelHandlerContext ctx) {
        List<String> lowSpeedData = new ArrayList<>();
        List<String> highSpeedData = new ArrayList<>();
        while (!dataQueue.isEmpty()) {
            String data = dataQueue.poll();
            int speed = decodeSpeed(data);
            if (speed <= 5) {
                lowSpeedData.add(data);
            } else {
                highSpeedData.add(data);
            }
        }
        for (String data : lowSpeedData) {
            processReceivedData(ctx, data);
        }
        for (String data : highSpeedData) {
            processReceivedData(ctx, data);
        }
    }

    private void processReceivedData(ChannelHandlerContext ctx, String receivedData) {
            String decodedData = decodeData(receivedData);
            saveDataToDatabase(decodedData);

            String decodeGeofence = decodeGeofence(receivedData);
            saveDecodedGeofenceData(decodeGeofence);

        if (isImeiExist(vehicleImei(receivedData))){
            updateVehicles(decodeImei(receivedData));
        }else {
            addVehicleIfNotExists(decodeImei(receivedData));
        }
    }

    private String vehicleImei(String data){
        try {
            String imei = data.substring(8, 8 + 15);

            return imei;
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String decodeImei(String data){
        String imei = data.substring(8, 8 + 15);
        return imei;
    }
    private String decodeGsmSignal(String data) {
        return data.substring(70, 70 + 2);
    }
    private int decodeSpeed(String data) {
        String intValue = data.substring(67, 67 + 3);
        return Integer.parseInt(intValue, 2);
    }
    private double decodeLatitude(String data) {
        String lat = data.substring(81, 81 + 9);
        int latDegrees = Integer.parseInt(lat.substring(0, 2));
        double latMinutes = Double.parseDouble(lat.substring(2)) / 1;
        return latDegrees + latMinutes / 60;
    }
    private double decodeLongitude(String data) {
        String lng = data.substring(91, 91 + 10);
        int lngDegrees = Integer.parseInt(lng.substring(0, 3));
        double lngMinutes = Double.parseDouble(lng.substring(3)) / 1;
        return lngDegrees + lngMinutes / 60;
    }
    private Timestamp decodeTimestamp(String data) {
        String dateTimeString = data.substring(34, 34 + 2) + data.substring(32, 32 + 2) +
                data.substring(36, 36 + 2) + data.substring(38, 38 + 2) +
                data.substring(40, 40 + 2) + data.substring(42, 42 + 2);
        int year = Integer.parseInt("20" + dateTimeString.substring(2, 4));
        int month = Integer.parseInt(dateTimeString.substring(0, 2));
        int day = Integer.parseInt(dateTimeString.substring(4, 6));
        int hour = Integer.parseInt(dateTimeString.substring(6, 8));
        int minute = Integer.parseInt(dateTimeString.substring(8, 10));
        int second = Integer.parseInt(dateTimeString.substring(10, 12));
        hour += 8;
        if (hour >= 24) {
            hour -= 24;
            Calendar calendar = Calendar.getInstance();
            calendar.set(year, month - 1, day, hour, minute, second);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            year = calendar.get(Calendar.YEAR);
            month = calendar.get(Calendar.MONTH) + 1;
            day = calendar.get(Calendar.DAY_OF_MONTH);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month - 1, day, hour, minute, second);
        if (hour >= 24) {
            calendar.add(Calendar.DAY_OF_MONTH, 1);
        }
        return new Timestamp(calendar.getTimeInMillis());
    }
    private String decodeStatus(String data, int speed) {
        String statusHex = data.substring(24, 24 + 8);
        String decimal = hexToBinary(statusHex);
        int statusConversion = Character.getNumericValue(decimal.charAt(1));
        return getStatus(statusConversion, speed);
    }
    private String decodeAlert(String data) {
        String alertOutput = data.substring(6, 6 + 2);
        switch (alertOutput) {
            case "21":
                return "Ignition on";
            case "22":
                return "Ignition off";
            case "10":
                return "GPS disconnected";
            case "11":
                return "External power reconnected";
            case "12":
                return "External power supply low level";
            case "13":
                return "GPS device battery low voltage";
            case "02":
                return "Over speeding";
            case "03":
                return "Speed recovered";
            case "06":
                return "Towed";
            case "40":
                return "Shock alarm";
            case "41":
                return "Excessive idling";
            case "42":
                return "Max acceleration";
            case "43":
                return "Hard braking";
            default:
                return "Normal data";
        }
    }
    private int getSetSpeedLimitId(String imei) {
        String selectSpeedQuery = "SELECT speed_limit_id FROM vehicles WHERE imei = ?";
        try {
            Integer speed_limit_id = jdbcTemplate.queryForObject(selectSpeedQuery, Integer.class, imei);
            return speed_limit_id != null ? speed_limit_id : -1; // Return -1 if no speed_limit_id found
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
    }

    private int getSetSpeedLimit(String data) {
        String selectSpeedQuery = "SELECT speed FROM speed_limits WHERE id = ?";
        try {
            String imei = decodeImei(data);
            int speed_limit_id = getSetSpeedLimitId(imei);
            if (speed_limit_id == -1) {
                // Handle the case where no speed_limit_id is found
                return 2000; // Return a default value
            }
            return jdbcTemplate.queryForObject(selectSpeedQuery, int.class, speed_limit_id);
        } catch (Exception e) {
            e.printStackTrace();
            return 2000;
        }
    }

    private String decodeDirection(String data) {
        String direction = data.substring(64, 64 + 3);
        int directionValue = Integer.parseInt(direction);
        return String.valueOf(getDirection(directionValue));
    }
    private String decodeDistanceKm(String data) {
        String distanceKm = data.substring(74, 74 + 7);
        int distanceKmValue = Integer.parseInt(distanceKm);
        return String.valueOf(getDistance(distanceKmValue));
    }
    private String hexToBinary(String hex) {
        int decimal = Integer.parseInt(hex, 16);
        String binary = String.format("%32s", Integer.toBinaryString(decimal)).replace(' ', '0');
        return binary;
    }
    private String getStatus(int status_conversion, int speed) {
        if (status_conversion == 0) {
            return "stopped";
        } else if (status_conversion == 1 && speed == 0) {
            return "idling";
        } else if (status_conversion == 1 && speed > 0) {

            return "driving";
        } else {
            return "unknown";
        }
    }
    //Decode the data{
    private String decodeData(String data) {
        try {
            String imei = decodeImei(data);
            String fuelValue = "0";
            String doorState = "0";
            String tirePreassure = "0";
            String tempSensor = "0";
            String brand = "0";
            String model = "0";
            String gsm = decodeGsmSignal(data);
            int speed  = decodeSpeed(data);
            int speedLimit = getSetSpeedLimit(data);
            //Create default date and time
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String created_at = now.format(formatter);
            String updated_at = now.format(formatter);
            //Decode lat and long
            double  latValue = decodeLatitude(data);
            double lngValue = decodeLongitude(data);
            //Decode status
            String vehicle_status = decodeStatus(data, speed);
            //Decode alert
            String alert = "";
            if (speed <= speedLimit){
                alert = decodeAlert(data);
            }else {
                alert = "Over speeding";
            }
            //Decode direction
            String direction = decodeDirection(data);
            //Decode distance_km
            String distance_km = decodeDistanceKm(data);
            //Decode timestamp
            java.sql.Timestamp timestamp = decodeTimestamp(data);

            String combinedAddress = "";
            String lastLocation = getLastLocation(imei);

            if (lastLocation != null) {
                combinedAddress = lastLocation;
            }
            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + combinedAddress + ";" + timestamp + ";" + vehicle_status + ";" + direction + ";" + distance_km  + ";" + speed + ";" + fuelValue
                    + ";" + doorState + ";" + tirePreassure + ";" + tempSensor + ";" + brand + ";" + model + ";" + created_at + ";" + updated_at + ";" + alert + ";" + gsm;
            return dataCombination;

        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }
    private String getDirection(int direction_value) {
        if (direction_value > 0) {
            direction_value = 0;
        }
        return String.valueOf(direction_value);
    }
    private String getDistance(int distance_km_value) {
        if (distance_km_value > 0) {
            distance_km_value = 0;
        }
        return String.valueOf(distance_km_value);
    }
    private String getLastLocation(String imei) {
        String selectPreviousLocation = "SELECT address FROM vehicle_logs WHERE imei = ? ORDER BY id DESC LIMIT 1";

        try {
            Map<String, Object> previousLog = jdbcTemplate.queryForMap(selectPreviousLocation, imei);
            if (previousLog != null && !previousLog.isEmpty()) {
                String address = (String) previousLog.get("address");
                return address;
            } else {
                return null;
            }
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            // Handle other exceptions
            e.printStackTrace();
            return null;
        }
    }
    private String saveDataToDatabase(String data) {
        String[] values = data.split(";");
        String query = "INSERT INTO vehicle_logs (imei,lat,`lng`,address,datetime,status,direction,distance,speed,fuel_level,door_estate,tire_pressure,temperature,brand,model,created_at,updated_at,alert,gsm_signal) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            jdbcTemplate.update(query, values);
            return "Data saved successfully to database.";
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to save data to database: " + e.getMessage();
        }
    }
    //}

    //Find geofence {
    private String decodeGeofence(String data) {
        try {
            String imei = decodeImei(data);
            int speed = decodeSpeed(data);
            double latValue = decodeLatitude(data);
            double lngValue = decodeLongitude(data);
            String vehicle_status = decodeStatus(data, speed);
            java.sql.Timestamp timestamp = decodeTimestamp(data);
            String direction = decodeDirection(data);
            int companyID = getCompanyId(imei);
            if (companyID <= 0 ) {
                return "Vehicle not exist, No Company detected";
            }

            boolean geofence_set = checkCompanyGeofence(companyID);
            boolean checkIfExistGeofenceLog = checkIfExistGeofenceLog(imei);
            String getLastReportGStatus = getLastReportGStatus(imei);
            String inOrOutInGeofence = inOrOutInGeofence(latValue, lngValue, companyID);
            int geofenceId = 0;
            int findGeofenceId = getGeofenceID(latValue, lngValue, companyID);
            int lastID = getLastKnownGeofenceID(imei);
            if(findGeofenceId != -1){
                geofenceId = findGeofenceId;
            }else {
                geofenceId = lastID;
            }
            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + direction + ";" + inOrOutInGeofence+ ";"  + timestamp + ";" + vehicle_status + ";" + geofenceId;

            if (speed >= 5) {
                if (geofence_set) {
                    if (checkIfExistGeofenceLog) {
                        String lastStatus = getLastReportGStatus.toLowerCase();
                        String currentStatus = inOrOutInGeofence.toLowerCase();
                        if (!lastStatus.equals(currentStatus)) {
                            return dataCombination;
                        } else {
                            return null;
                        }
                    } else {
                        return dataCombination;
                    }
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void saveDecodedGeofenceData(String decodedGeofenceData) {
        if (decodedGeofenceData != null) {
            String[] values = decodedGeofenceData.split(";");
            if (values.length >= 8) { // Ensure that the array has at least 8 elements
                String query = "INSERT INTO geofence_logs (imei, lat, lng, direction, gstatus, date_time, vstatus, geofence_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                try {
                    jdbcTemplate.update(query, values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]);
                    System.out.println("Data saved successfully in geofence log.");
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Failed to save data in geofence log: " + e.getMessage());
                }
            } else {
                System.out.println("IMEI not found to set geofence.");
            }
        } else {
            System.out.println("Decoded geofence data is null.");
        }
    }


    private String inOrOutInGeofence(double lat, double lng, int companyID) {
        List<String> polygonCoordinates = jdbcTemplate.queryForList("SELECT coordinates FROM geofences WHERE company_id = ?", String.class, companyID);

        for (String coordinates : polygonCoordinates) {
            if (isPointInsideGeofencePolygon(lat, lng, coordinates)) {
                return "in";
            }
        }
        return "out";
    }
    private int getGeofenceID(double lat, double lng, int companyID) {
        List<Map<String, Object>> geofences = jdbcTemplate.queryForList("SELECT id, coordinates FROM geofences WHERE company_id = ?", companyID);

        for (Map<String, Object> geofence : geofences) {
            String coordinates = (String) geofence.get("coordinates");
            BigInteger geofenceIDBigInt = (BigInteger) geofence.get("id");

            // Convert BigInteger to int
            int geofenceID = geofenceIDBigInt.intValue();

            if (isPointInsideGeofencePolygon(lat, lng, coordinates)) {
                return geofenceID;
            }
        }
        // Returning -1 when no geofence is found
        return -1;
    }

    private int getLastKnownGeofenceID(String imei) {
        String sql = "SELECT geofence_id FROM geofence_log WHERE imei = ? ORDER BY id DESC LIMIT 1";

        try {
            int lastKnownGeofenceID = jdbcTemplate.queryForObject(sql, Integer.class, imei);
            return lastKnownGeofenceID;
        } catch (EmptyResultDataAccessException e) {
            // Handle case where no logs are found
            return -1; // or whatever sentinel value you prefer
        }
    }

    private boolean checkIfExistGeofenceLog(String imei) {
        String query = "SELECT COUNT(*) FROM geofence_log WHERE imei = ?";
        int count = jdbcTemplate.queryForObject(query, Integer.class, imei);
        return count > 0;
    }
    private boolean isPointInsideGeofencePolygon(double lat, double lng, String coordinates) {
        // Parse coordinates from string format
        String[] points = coordinates.trim().split(",");
        List<Double> polygonLat = new ArrayList<>();
        List<Double> polygonLng = new ArrayList<>();
        for (String point : points) {
            String[] parts = point.trim().split("\\s+");
            double latitude = Double.parseDouble(parts[0]);
            double longitude = Double.parseDouble(parts[1]);
            polygonLat.add(latitude);
            polygonLng.add(longitude);
        }
        int n = polygonLat.size();
        int i, j;
        boolean isInside = false;
        for (i = 0, j = n - 1; i < n; j = i++) {
            if ((polygonLat.get(i) > lat) != (polygonLat.get(j) > lat) &&
                    (lng < (polygonLng.get(j) - polygonLng.get(i)) * (lat - polygonLat.get(i)) / (polygonLat.get(j) - polygonLat.get(i)) + polygonLng.get(i))) {
                isInside = !isInside;
            }
        }
        return isInside;
    }
    private boolean checkCompanyGeofence(int companyId) {
        String query = "SELECT COUNT(*) FROM geofences WHERE company_id = ?";
        int count = jdbcTemplate.queryForObject(query, Integer.class, companyId);
        return count > 0;
    }
    public String getLastReportGStatus(String imei) {
        String query = "SELECT gstatus FROM geofence_log WHERE imei = ? ORDER BY id DESC LIMIT 1";
        List<String> results = jdbcTemplate.queryForList(query, String.class, imei);
        if (results.isEmpty()) {
            System.err.println("No result found for IMEI: " + imei);
            return null; // or return a default value
        } else {
            return results.get(0);
        }
    }
    private int getCompanyId(String imei) {
        String query = "SELECT company_id FROM vehicles WHERE imei = ?";
        try {
            Integer companyId = jdbcTemplate.queryForObject(query, Integer.class, imei);
            return (companyId != null) ? companyId : -1; // Return -1 if companyId is null
        } catch (EmptyResultDataAccessException e) {
            return -1; // Return -1 if no result found
        }
//}
    }
    public void updateVehicles(String imei) {
        String updateVehiclesQuery = "UPDATE vehicles SET v_log_id = (SELECT MAX(id) FROM vehicle_logs WHERE imei = ?) WHERE imei = ?";
        try {
            jdbcTemplate.update(updateVehiclesQuery, imei, imei);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void addVehicleIfNotExists(String imei) {
        Integer companyId = null; // Set company_id to null
        String query = "INSERT INTO vehicles (company_id, platenumber, model, imei, sim_num, driver_id, vehicle_type_id, body_num, chassis_num, engine_num, color, e_fuel_cons, date_purchased,is_registered, v_log_id) " +
                "SELECT ?, null, null, ?, null, null, 1, null, null, null, null, null, null, null, null " +
                "FROM DUAL WHERE NOT EXISTS (SELECT * FROM vehicles WHERE imei = ?)";
        int retryAttempts = 3;
        for (int i = 0; i < retryAttempts; i++) {
            try {
                jdbcTemplate.update(query, companyId, imei, imei);
                return; // Exit the method if the operation is successful
            } catch (CannotAcquireLockException e) {
                if (i < retryAttempts - 1) {
                    // Log the retry attempt
                    System.out.println("Retrying database operation due to deadlock...");
                    // Wait for a short delay before retrying
                    try {
                        Thread.sleep(100); // Adjust the delay time as needed
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    // Log or handle the failure after all retry attempts
                    e.printStackTrace();
                }
            }
        }
    }
    public boolean isImeiExist(String imei) {
        String sql = "SELECT COUNT(*) FROM vehicles WHERE imei = ?";
        int count = jdbcTemplate.queryForObject(sql, Integer.class, imei);
        return count > 0;
    }
    private void saveDataToNotepad(String receivedData){
        writeDataToNotepad(receivedData);
    }
    private void writeDataToNotepad(String receivedData) {
        String filePath = "C:\\PortRunner/output.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            writer.write(receivedData + "\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        executorGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }
}
