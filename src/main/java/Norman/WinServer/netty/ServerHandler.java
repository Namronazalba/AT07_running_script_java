package Norman.WinServer.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.springframework.dao.CannotAcquireLockException;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private static final EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(8);
    private final JdbcTemplate jdbcTemplate;
    private final int batchSize = 1;

    private final Queue<String> dataQueue = new LinkedList<>();
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
        try {
            byte[] buffer = new byte[in.readableBytes()];
            in.readBytes(buffer);
            String receivedData = new String(buffer, StandardCharsets.UTF_8);
            dataQueue.offer(receivedData);
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
            int speed = extractSpeed(data);
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

    private int extractSpeed(String receivedData) {
        String intValue = receivedData.substring(67, 67 + 3);
        return Integer.parseInt(intValue, 2);
    }

    private void processReceivedData(ChannelHandlerContext ctx, String receivedData) {
        String intValue = receivedData.substring(67, 67 + 3);
        int speed = Integer.parseInt(intValue, 2);

        if (speed <= 5) {
            String decodedData = decodeData(receivedData);
            saveDataToDatabase(decodedData);
            geofencing(receivedData);
        }else {
            saveDataString(receivedData);
        }

        if (isImeiExist(vehicleImei(receivedData))){
            updateVehicles(vehicleImei(receivedData));
        }else {
            addVehicleIfNotExists(vehicleImei(receivedData));
        }
    }

    private void saveDataString(String data) {
        String[] values = data.split(";");
        String query = "INSERT INTO tbl_data_string (data) VALUES (?)";
        try {
            jdbcTemplate.update(query,values);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //Decode the data{
    private String decodeData(String data) {
        try {
            String imei = data.substring(8, 8 + 15);
            String fuelValue = "0";
            String doorState = "0";
            String tirePreassure = "0";
            String tempSensor = "0";
            String brand = "0";
            String model = "0";
            String gsm = data.substring(70, 70 + 2);
            String intValue = data.substring(67, 67 + 3);
            int speed  = Integer.parseInt(intValue, 2);

            //Create default date and time
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String created_at = now.format(formatter);
            String updated_at = now.format(formatter);

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

            java.sql.Timestamp timestamp = new java.sql.Timestamp(calendar.getTimeInMillis());


            String combinedAddress = "";
            String lastLocation = getLastLocation(imei);

            if (lastLocation == null) {
                int latInt = (int) latValue;
                int lngInt = (int) lngValue;
                int radius = 20;
//                String poiData = getPois(latInt, lngInt);
//                String nearestCoordinates = findNearestPOI(latValue, lngValue, poiData);
//                String address = getPoiNameForCoordinates(nearestCoordinates);

                List<Map<String, Object>> matchedRoads = queryDatabaseForMatchedRoads(latInt, lngInt);
                List<Map<String, Object>> filteredCoords = filterMatchedRoads(matchedRoads, latValue, lngValue);
                List<String> formattedCoordsList = new ArrayList<>();
                for (Map<String, Object> road : filteredCoords) {
                    String coords = (String) road.get("coords");
                    String formattedCoords = formatCoordinates(coords);
                    formattedCoordsList.add(formattedCoords);
                }

                double[] nearestCoords = extractNearestCoords(formattedCoordsList, latValue, lngValue, radius);
                String roadName = "";
                if (nearestCoords != null) {
                    roadName = fetchRoadsMatchingCoordinates(nearestCoords);
                }

                String[] result = {"", "", "", ""};
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

//                if (!address.isEmpty()) {
//                    combinedAddress += address + ",";
//                }
                // Check if road is not empty
                if (!roadName.isEmpty()) {
                    combinedAddress += roadName + ",";
                }
                combinedAddress += locationFound;

            }else {
                combinedAddress = lastLocation;
            }
            System.out.println(combinedAddress);
            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + combinedAddress + ";" + timestamp + ";" + vehicle_status + ";" + direction + ";" + distance_km  + ";" + speed + ";" + fuelValue
                    + ";" + doorState + ";" + tirePreassure + ";" + tempSensor + ";" + brand + ";" + model + ";" + created_at + ";" + updated_at + ";" + alert + ";" + gsm;
            return dataCombination;

        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static final double RADIUS_OF_EARTH = 6371000;
    public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2)
                * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = RADIUS_OF_EARTH * c;

        return distance;
    }

    public static String findNearestPOI(double targetLat, double targetLong, String poiData) {
        String[] poiCoordinates = poiData.split(",");
        double minDistance = Double.MAX_VALUE;
        String nearestPOI = "";
        for (String poi : poiCoordinates) {
            String[] poiParts = poi.split(" ");
            double poiLat = Double.parseDouble(poiParts[0]);
            double poiLong = Double.parseDouble(poiParts[1]);

            double distance = calculateDistance(targetLat, targetLong, poiLat, poiLong);
            if (distance < minDistance) {
                minDistance = distance;
                nearestPOI = poi;
            }
        }
        return nearestPOI;
    }
    public String getPois(int clatInt, int clngInt) {
        String getClatClng = "SELECT clat, clng FROM tbl_allpois WHERE CAST(clat AS UNSIGNED) = ? AND CAST(clng AS UNSIGNED) = ?";
        List<Map<String, Object>> poiRows = jdbcTemplate.queryForList(getClatClng, clatInt, clngInt);
        StringBuilder pointsBuilder = new StringBuilder();
        for (Map<String, Object> row : poiRows) {
            double clat = Double.parseDouble(row.get("clat").toString());
            double clng = Double.parseDouble(row.get("clng").toString());
            pointsBuilder.append(clat).append(" ").append(clng).append(",");
        }
        if (pointsBuilder.length() > 0) {
            pointsBuilder.setLength(pointsBuilder.length() - 1);
        }
        return pointsBuilder.toString();
    }
    public String getPoiNameForCoordinates(String coordinates) {
        String[] coordParts = coordinates.split(" ");
        double clat = Double.parseDouble(coordParts[0]);
        double clng = Double.parseDouble(coordParts[1]);
        String getNameQuery = "SELECT name FROM tbl_allpois WHERE clat = ? AND clng = ?";
        try {
            String poiName = jdbcTemplate.queryForObject(getNameQuery, String.class, clat, clng);
            return poiName;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    private List<Map<String, Object>> filterMatchedRoads(List<Map<String, Object>> matchedRoads, double latValue, double lngValue) {
        double truncatedLat = Math.floor(latValue * 100) / 100;
        double truncatedLng = Math.floor(lngValue * 100) / 100;

        List<Map<String, Object>> filteredCoords = new ArrayList<>();

        for (Map<String, Object> road : matchedRoads) {
            String coords = (String) road.get("coords");
            String[] coordPairs = coords.split(",");

            Map<String, Object> filteredMap = new HashMap<>();
            StringBuilder filteredCoordsBuilder = new StringBuilder();

            for (String coordPair : coordPairs) {
                String[] latLng = coordPair.trim().split(" ");
                double lat = Double.parseDouble(latLng[0]);
                double lng = Double.parseDouble(latLng[1]);

                double truncatedCoordLat = Math.floor(lat * 100) / 100;
                double truncatedCoordLng = Math.floor(lng * 100) / 100;

                if (truncatedCoordLat == truncatedLat && truncatedCoordLng == truncatedLng) {
                    filteredCoordsBuilder.append(coordPair).append(",");
                }
            }

            if (filteredCoordsBuilder.length() > 0) {
                // Remove the trailing comma
                String filteredCoordsString = filteredCoordsBuilder.substring(0, filteredCoordsBuilder.length() - 1);
                filteredMap.put("coords", filteredCoordsString);
                filteredCoords.add(filteredMap);
            }
        }

        return filteredCoords;
    }

    private List<Map<String, Object>> queryDatabaseForMatchedRoads(double latValue, double lngValue) {
        int latInt = (int) latValue;
        int lngInt = (int) lngValue;

        String selectAllQuery = "SELECT coords FROM tbl_allroads WHERE lat = ? AND lng = ?";
        return jdbcTemplate.queryForList(selectAllQuery, latInt, lngInt);
    }
    private String formatCoordinates(String coords) {
        StringBuilder formattedCoords = new StringBuilder();
        String[] points = coords.split(",");
        for (String point : points) {
            String[] latLng = point.trim().split(" ");
            formattedCoords.append("{")
                    .append(latLng[0].trim()).append(", ")
                    .append(latLng[1].trim())
                    .append("}, ");
        }
        // Remove the trailing comma and space
        if (formattedCoords.length() > 0) {
            formattedCoords.delete(formattedCoords.length() - 2, formattedCoords.length());
        }
        return formattedCoords.toString();
    }

    private double[] extractNearestCoords(List<String> formattedCoordsList, double latValue, double lngValue, int radius) {
        double minDistance = Double.MAX_VALUE;
        double[] nearestCoords = null;
        for (String formattedCoords : formattedCoordsList) {
            String[] parts = formattedCoords.split("\\}, \\{"); // Split by "}, {"
            for (String part : parts) {
                String[] latLng = part.replaceAll("[{}]", "").split(", "); // Remove braces and split by ", "
                double lat = Double.parseDouble(latLng[0]);
                double lng = Double.parseDouble(latLng[1]);
                double distance = calculateDistance(lat, lng, latValue, lngValue);
                if (distance <= radius) {
                    if (distance < minDistance) {
                        minDistance = distance;
                        nearestCoords = new double[]{lat, lng};
                    }
                }
            }
        }
        return nearestCoords;
    }

    private String fetchRoadsMatchingCoordinates(double[] nearestCoords) {
        String coordsString = nearestCoords[0] + " " + nearestCoords[1];
        String selectQuery = "SELECT roads FROM tbl_allroads WHERE coords LIKE ? LIMIT 1";
        List<Map<String, Object>> roadsResult = jdbcTemplate.queryForList(selectQuery, "%" + coordsString + "%");
        List<String> roads = roadsResult.stream()
                .map(row -> (String) row.get("roads"))
                .collect(Collectors.toList());

        return String.join(", ", roads);
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
                String not_exist = "Vehicle not exist, No Company detected";
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
        List<String> polygonCoordinates = jdbcTemplate.queryForList("SELECT coordinates FROM geofences WHERE company_id = ?", String.class, companyID);

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
                "SELECT coordinates, place_name FROM geofences",
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
    private boolean checkCompanyGeofence(String companyId) {
        String query = "SELECT COUNT(*) FROM geofences WHERE company_id = ?";
        int count = jdbcTemplate.queryForObject(query, Integer.class, companyId);
        return count > 0;
    }
    public String getLastReportGStatus(String imei) {
        String query = "SELECT gstatus FROM geofence_log WHERE imei = ? ORDER BY date_time DESC LIMIT 1";
        try {
            return jdbcTemplate.queryForObject(query, String.class, imei);
        } catch (EmptyResultDataAccessException e) {
            e.printStackTrace();
            return null;
        }
    }
    public void saveDataInGeofenceLog(String imei, String gstatus, Timestamp timestamp, String vstatus, String address) {
        String query = "INSERT INTO geofence_log (imei, gstatus, date_time, vstatus, address) VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(query, imei, gstatus, timestamp,  vstatus, address);
    }
    private String getCompanyId(String imei) {
        String query = "SELECT company_id FROM vehicles WHERE imei = ?";
        try {
            return jdbcTemplate.queryForObject(query, String.class, imei);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
//}
    private void saveDataToDatabase(String data) {
        String[] values = data.split(";");
        String query = "INSERT INTO vehicle_logs (imei,lat,`lng`,address,datetime,status,direction,distance,speed,fuel_level,door_estate,tire_pressure,temperature,brand,model,created_at,updated_at,alert,gsm_signal) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            jdbcTemplate.update(query, values);
        } catch (Exception e) {
            e.printStackTrace();
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
        String query = "INSERT INTO vehicles (company_id, platenumber, model, imei, sim_num, driver_id, vehicle_type, body_num, chassis_num, engine_num, color, e_fuel_cons, date_purchased,is_registered, v_log_id) " +
                "SELECT ?, null, null, ?, null, null, 'Sedan', null, null, null, null, null, null, null, null " +
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
