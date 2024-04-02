package Norman.WinServer.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private static final EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(8);
    private final JdbcTemplate jdbcTemplate;
    public ServerHandler() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//        dataSource.setUrl("jdbc:mysql://localhost:3306/db_ezpht?useSSL=false");
        dataSource.setUrl("jdbc:mysql://192.168.1.242:3306/db_ezpht?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("pass");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        StringBuilder receivedData = new StringBuilder();
        try {
            byte[] buffer = new byte[in.readableBytes()];
            in.readBytes(buffer);
            receivedData.append(new String(buffer, StandardCharsets.UTF_8));
        } finally {
            in.release();
        }
//        saveDataToNotepad(receivedData.toString());

        String decodedData = decodeData(receivedData.toString());
        String geofencingData = geofencing(receivedData.toString());
        if (decodedData != null) {
            saveDataToDatabase(decodedData);
            System.out.println(geofencingData);
        }
    }

    //Decode the data{
    private String decodeData(String data) {
        try {
            String imei = data.substring(8, 8 + 15);
            String fuelValue = "0";
            String colorState = "0";
            String tirePreassure = "0";
            String tempSensor = "0";
            String brand = "0";
            String model = "0";
            String gsm = data.substring(70, 70 + 2);
            String intValue = data.substring(67, 67 + 3);
            int speed  = Integer.parseInt(intValue, 2);

            //Decode lat and long
            String lat = data.substring(81, 81 + 9);
            int lat_degrees = Integer.parseInt(lat.substring(0, 2));
            double lat_minutes = Double.parseDouble(lat.substring(2)) / 1;
            String l0ng = data.substring(91, 91 + 10);
            int lng_degrees = Integer.parseInt(l0ng.substring(0, 3));
            double lng_minutes = Double.parseDouble(l0ng.substring(3)) / 1;
            double  latValue = lat_degrees + lat_minutes / 60;
            double lngValue = lng_degrees + lng_minutes / 60;

            //Decode status
            String statusHex = data.substring(24, 24 + 8);
            String decimal = hexToBinary(statusHex );
            int status_conversion = Character.getNumericValue(decimal.charAt(1));
            String vehicle_status = getStatus(status_conversion, speed);
            String alert_output = data.substring(6, 6 + 2);
            String alert;
            switch (alert_output) {
                case "21":
                    alert = "ignition on";
                    break;
                case "22":
                    alert = "ignition off";
                    break;
                case "10":
                    alert = "GPS disconnected";
                    break;
                case "11":
                    alert = "External power reconnected";
                    break;
                case "12":
                    alert = "External power supply low level";
                    break;
                case "13":
                    alert = "GPS device battery low voltage";
                    break;
                case "02":
                    alert = "Over speeding";
                    break;
                case "03":
                    alert = "Speed recovered";
                    break;
                case "06":
                    alert = "Towed";
                    break;
                case "40":
                    alert = "Shock alarm";
                    break;
                case "41":
                    alert = "Excessive idling";
                    break;
                case "42":
                    alert = "Max acceleration";
                    break;
                case "43":
                    alert = "Hard braking";
                    break;
                default:
                    alert = "Normal data";
                    break;
            }

            //Decode direction
            String direction = data.substring(64, 64 + 3);
            int direction_value = Integer.parseInt(direction);
            direction = String.valueOf(getDirection(direction_value));

            //Decode distance_km
            String distance_km = "";
            distance_km = data.substring(74, 74 + 7);
            int distance_km_value = Integer.parseInt(distance_km);
            distance_km = String.valueOf(getDistance(distance_km_value));

            //Decode Time and Date
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
            calendar.add(Calendar.HOUR_OF_DAY, 8);
            java.sql.Timestamp timestamp = new java.sql.Timestamp(calendar.getTimeInMillis());
            int latInt = (int) latValue;
            int lngInt = (int) lngValue;
            String combinedAddress = "";
            String lastLocation = getLastLocation(imei);
            if (speed <= 5 && imeiExists(imei)) {
                combinedAddress = lastLocation;
            }else {
                //Find nearest POIS
                String selectQuery = "SELECT name, clat, clng, " +
                        "(6371000 * acos(cos(radians(?)) * cos(radians(clat)) * cos(radians(clng) - radians(?)) + sin(radians(?)) * sin(radians(clat)))) AS distance " +
                        "FROM (SELECT name, clat, clng FROM tbl_allpois ORDER BY " +
                        "(6371000 * acos(cos(radians(?)) * cos(radians(clat)) * cos(radians(clng) - radians(?)) + sin(radians(?)) * sin(radians(clat)))) LIMIT 1) AS nearest_location " +
                        "HAVING distance <= 30 " +
                        "ORDER BY distance LIMIT 1";

                List<Map<String, Object>> rows = jdbcTemplate.queryForList(selectQuery, latValue, lngValue, latValue, latValue, lngValue, latValue);
                String address = "";
                if (!rows.isEmpty()) {
                    Map<String, Object> nearestLocation = rows.get(0);
                    address = (String) nearestLocation.get("name");
                }

                // Find nearest ROAD
                String selectAllroads = "SELECT id, roads, coords FROM tbl_allroads WHERE lat = ? AND lng = ?";
                List<String> coordinatesAllroads = jdbcTemplate.queryForList(selectAllroads, latInt, lngInt)
                        .stream()
                        .map(row -> (String) row.get("coords"))
                        .flatMap(coords -> Arrays.stream(coords.split(",")))
                        .collect(Collectors.toList());

                String allRoadsCoordinates = String.join(",", coordinatesAllroads);
                String[] coordinateAllRoadsPairs = allRoadsCoordinates.split(",");
                String nearestCoordinate = Arrays.stream(coordinateAllRoadsPairs)
                        .map(pair -> pair.trim().split("\\s+"))
                        .filter(latLng -> latLng.length == 2)
                        .min(Comparator.comparingDouble(latLng -> {
                            double coordLat = Double.parseDouble(latLng[0]);
                            double coordLng = Double.parseDouble(latLng[1]);
                            return Math.sqrt(Math.pow(latValue - coordLat, 2) + Math.pow(lngValue - coordLng, 2));
                        }))
                        .map(latLng -> String.join(" ", latLng))
                        .orElse("");
                double minDistance = Double.MAX_VALUE;
                if (!nearestCoordinate.isEmpty()) {
                    String[] latLng = nearestCoordinate.split("\\s+");
                    double coordLat = Double.parseDouble(latLng[0]);
                    double coordLng = Double.parseDouble(latLng[1]);

                    minDistance = Math.sqrt(Math.pow(latValue - coordLat, 2) + Math.pow(lngValue - coordLng, 2));
                }
                if (minDistance > 0.0002809252747641406233829 || nearestCoordinate.isEmpty()) {
                    nearestCoordinate = "";
                }
                String selectNameQuery = "SELECT * FROM `tbl_allroads` WHERE coords LIKE ?";
                List<Map<String, Object>> nameResult = jdbcTemplate.queryForList(selectNameQuery,nearestCoordinate);
                String roadName = nameResult.stream()
                        .findFirst()
                        .map(row -> (String) row.get("roads"))
                        .orElse("");

                //Find location in LUZVIMIN
                String[] result = { "", "", "", "" };

                String selectQueryLuzvimin = "SELECT id, address, add_ext, coords FROM tbl_luzvimin WHERE lat = ? AND lng = ?";
                List<Map<String, Object>> selectedLuzvimin = jdbcTemplate.queryForList(selectQueryLuzvimin, latInt, lngInt);

                selectedLuzvimin.stream()
                        .map(row -> new Object[]{
                                String.valueOf(row.get("id")),
                                String.valueOf(row.get("coords")),
                                row.get("address"),
                                row.get("add_ext")
                        })
                        .filter(objects -> isPointInPolygon(latValue, lngValue, (String) objects[1]))
                        .findFirst()
                        .ifPresent(objects -> {
                            result[0] = (String) objects[0];
                            result[1] = String.valueOf(objects[2]);
                            result[2] = String.valueOf(objects[3]);
                            result[3] = String.valueOf(objects[2]) + String.valueOf(objects[3]);
                        });
                String locationFound = result[3];
                // Check if address is not empty
                if (!address.isEmpty()) {
                    combinedAddress += address + ",";
                }
                // Check if road is not empty
                if (!roadName.isEmpty()) {
                    combinedAddress += roadName + ",";
                }
                combinedAddress += locationFound;
            }
            //System.out.println(combinedAddress);
            //System.out.println("==========================================");
            System.out.println("IMEI :" + imei);
            //System.out.println("Lat :" + latValue);
            //System.out.println("Long :" + lngValue);
            //System.out.println("Timestamp :" + timestamp);
            //System.out.println("Status :" + vehicle_status);
            //System.out.println("Status :" + alert);
            //System.out.println("Direction :" + direction);
            //System.out.println("Distance km :" + distance_km);
            //System.out.println("Speed :" + speed);
            //System.out.println("Fuel value :" + fuelValue);
            //System.out.println("Color state :" + colorState);
            //System.out.println("Tire preassure :" + tirePreassure);
            //System.out.println("Temp sensor :" + tempSensor);
            //System.out.println("Brand :" + brand);
            //System.out.println("Model :" + model);
            //System.out.println("GSM :" + gsm);
            //System.out.println("Address :" + combinedAddress);
            //System.out.println("==========================================");
            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + timestamp + ";" + vehicle_status + ";" + alert + ";" + direction + ";" + distance_km + ";" + speed + ";"
                    + fuelValue + ";" + colorState + ";" + tirePreassure + ";" + tempSensor + ";" + brand + ";" + model + ";" + gsm + ";" + combinedAddress;
            return dataCombination;
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
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
    public static boolean isPointInPolygon(double latValue, double lngValue, String coords) {
        String[] coordPairs = coords.split(",");
        int numPoints = coordPairs.length;
        double[] latitudes = new double[numPoints];
        double[] longitudes = new double[numPoints];
        IntStream.range(0, numPoints)
                .forEach(i -> {
                    String[] latLng = coordPairs[i].trim().split("\\s+");
                    latitudes[i] = Double.parseDouble(latLng[0]);
                    longitudes[i] = Double.parseDouble(latLng[1]);
                });
        AtomicInteger crossings = new AtomicInteger();
        IntStream.range(0, numPoints)
                .forEach(i -> {
                    int next = (i + 1) % numPoints;
                    if (((latitudes[i] <= latValue && latValue < latitudes[next]) ||
                            (latitudes[next] <= latValue && latValue < latitudes[i])) &&
                            (lngValue < (longitudes[next] - longitudes[i]) * (latValue - latitudes[i]) / (latitudes[next] - latitudes[i]) + longitudes[i])) {
                        crossings.incrementAndGet();
                    }
                });
        return crossings.get() % 2 != 0;
    }
    private boolean imeiExists(String imei) {
        String selectCount = "SELECT COUNT(*) FROM tbl_vehicle_logs WHERE imei = ?";
        try {
            int count = jdbcTemplate.queryForObject(selectCount, Integer.class, imei);
            return count > 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    private String getLastLocation(String imei) {
        String selectPreviousLocation = "SELECT address FROM tbl_vehicle_logs WHERE imei = ? ORDER BY id DESC LIMIT 1";

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
//}

//Find geofence {
    private String geofencing(String data){
        try {
            String imei = data.substring(8, 8 + 15);
//            String imei = "v861013034878893";
            String intValue = data.substring(67, 67 + 3);
            int speed  = Integer.parseInt(intValue, 2);
//            int speed  = 6;
            //Decode lat and long
            String lat = data.substring(81, 81 + 9);
            int lat_degrees = Integer.parseInt(lat.substring(0, 2));
            double lat_minutes = Double.parseDouble(lat.substring(2)) / 1;
            String l0ng = data.substring(91, 91 + 10);
            int lng_degrees = Integer.parseInt(l0ng.substring(0, 3));
            double lng_minutes = Double.parseDouble(l0ng.substring(3)) / 1;
            double  latValue = lat_degrees + lat_minutes / 60;
            double lngValue = lng_degrees + lng_minutes / 60;
//            double  latValue = Double.parseDouble("14.541428");
//            double lngValue = Double.parseDouble("120.978867");
            //Decode status
            String statusHex = data.substring(24, 24 + 8);
            String decimal = hexToBinary(statusHex );
            int status_conversion = Character.getNumericValue(decimal.charAt(1));
            String vehicle_status = getStatus(status_conversion, speed);
            //Decode Time and Date
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
            calendar.add(Calendar.HOUR_OF_DAY, 8);
            java.sql.Timestamp timestamp = new java.sql.Timestamp(calendar.getTimeInMillis());
            // Get geofence address
            String address = getGeofenceAddress(latValue, lngValue);
            // Get company id of data string imei
            var companyID = getCompanyId(imei);
            if (companyID == null) {
                String not_exist = "Vehicle not exist";
                return not_exist;
            }
            // Check if company set geofence
            var geofence_set = checkCompanyGeofence(companyID);
            // Check if existing in geofence log
            var checkIfExistGeofenceLog = checkIfExistGeofenceLog(imei);
            // get last report status on geofence log
            var getLastReportGStatus = getLastReportGStatus(imei);
            // Check if in or out on geofence polygon
            var inOrOutInGeofence = inOrOutInGeofence(latValue, lngValue, companyID);
            if (speed >= 5) {
                if (geofence_set){
                    if (checkIfExistGeofenceLog){
                        if (getLastReportGStatus != inOrOutInGeofence){
                            //alert and save in/out
                            saveDataInGeofenceLog(imei, inOrOutInGeofence, timestamp, vehicle_status, address);
                            System.out.println(imei + "+" + inOrOutInGeofence + "+" + vehicle_status + "+" + address);
                        }else {
                            return null;
                        }
                    }else {
                        if (inOrOutInGeofence == "in"){
                            saveDataInGeofenceLog(imei, inOrOutInGeofence, timestamp,vehicle_status, address);
                            System.out.println(imei + "+" + inOrOutInGeofence + "+" + vehicle_status + "+" + address);
                        }else {
                            saveDataInGeofenceLog(imei, inOrOutInGeofence, timestamp, vehicle_status, address);
                            System.out.println(imei + "+" + inOrOutInGeofence + "+" + vehicle_status + "+" + address);
                        }
                    }
                }else{
                    return null;
                }
            }else {
                return null;
            }
            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + timestamp + ";" + vehicle_status + ";" + speed;
            return dataCombination;
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }
    private String inOrOutInGeofence(double lat, double lng, String companyID) {
        List<String> polygonCoordinates = jdbcTemplate.queryForList("SELECT coordinates FROM tbl_geofences WHERE company_id = ?", String.class, companyID);

        for (String coordinates : polygonCoordinates) {
            if (isPointInsideGeofencePolygon(lat, lng, coordinates)) {
                return "in";
            }
        }
        return "out";
    }
    private String getGeofenceAddress(double lat, double lng) {
        String address = null;
        List<String[]> polygonsAndAddresses = jdbcTemplate.query(
                "SELECT coordinates, place_name FROM tbl_geofences",
                new RowMapper<String[]>() {
                    @Override
                    public String[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        String coordinates = rs.getString("coordinates");
                        String placeName = rs.getString("place_name");
                        return new String[]{coordinates, placeName};
                    }
                });
        for (String[] polygonAndAddress : polygonsAndAddresses) {
            String coordinates = polygonAndAddress[0];
            address = polygonAndAddress[1];
            if (isPointInsideGeofencePolygon(lat, lng, coordinates)) {
                return address;
            }
        }
        return address;
    }
    private boolean checkIfExistGeofenceLog(String imei) {
        String query = "SELECT COUNT(*) FROM tbl_geofence_log WHERE imei = ?";
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
    private boolean checkCompanyGeofence(String companyId) {
        String query = "SELECT COUNT(*) FROM tbl_geofences WHERE company_id = ?";
        int count = jdbcTemplate.queryForObject(query, Integer.class, companyId);
        return count > 0;
    }
    public String getLastReportGStatus(String imei) {
        String query = "SELECT gstatus FROM tbl_geofence_log WHERE imei = ? ORDER BY datetime DESC LIMIT 1";
        try {
            return jdbcTemplate.queryForObject(query, String.class, imei);
        } catch (EmptyResultDataAccessException e) {
            e.printStackTrace();
            return null;
        }
    }
    public void saveDataInGeofenceLog(String imei, String gstatus, Timestamp timestamp, String vstatus, String address) {
        String query = "INSERT INTO tbl_geofencelog_v2 (imei, gstatus, date_time, vstatus, address) VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(query, imei, gstatus, timestamp,  vstatus, address);
    }
    private String getCompanyId(String imei) {
        String query = "SELECT accnt_id FROM tbl_cvehicles WHERE imei = ?";
        try {
            return jdbcTemplate.queryForObject(query, String.class, imei);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
//}

    private void saveDataToDatabase(String data) {
        String query = "INSERT INTO tbl_vehicle_logs (imei,lat,`lng`,date_time,`status`, alert,direction,distance,speed,fuel_value,color_state,tire_preassure,temp_sensor,brand,model,gsm_signal,address) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String[] values = data.split(";");
        try {
            jdbcTemplate.update(query,
                    values[0],  // imei
                    values[1],  // lat
                    values[2],  // long
                    values[3],  // date_time
                    values[4],  // status
                    values[5],  // alert
                    values[6],  // direction
                    values[7],  // distance
                    values[8],  // speed
                    values[9],  // fuel_value
                    values[10], // color_state
                    values[11], // tire_preassure
                    values[12], // temp_sensor
                    values[13], // brand
                    values[14], // model
                    values[15], // gsm_signal
                    values[16]);// address

            String updateVehiclesQuery = "UPDATE tbl_vehicles SET vl_id = (SELECT MAX(id) FROM tbl_vehicle_logs WHERE imei = ?) WHERE imei = ?";
            jdbcTemplate.update(updateVehiclesQuery, values[0], values[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
