package Norman.WinServer.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private static final EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(4);

    private final JdbcTemplate jdbcTemplate;
    public ServerHandler() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://192.168.1.242:3306/db_ezpht?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("pass");

        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        String receivedData = in.toString(StandardCharsets.UTF_8);

        Executor executor = Executors.newCachedThreadPool();
        executor.execute(() -> {
            String decodedData = decodeData(receivedData);

            if (decodedData != null) {
                saveDataToDatabase(decodedData);
            }
        });
    }
    private String decodeData(String data) {
        try {
            String imei = data.substring(8, 8 + 15);
//                String imei = "868028032334701";
            String fuelValue = "0";
            String colorState = "0";
            String tirePreassure = "0";
            String tempSensor = "0";
            String brand = "0";
            String model = "0";
            String gsm = data.substring(70, 70 + 2);
            String speed = "1";

            //Decode lat and long
            String lat = data.substring(89, 89 + 9);
            int lat_degrees = Integer.parseInt(lat.substring(0, 2));
            double lat_minutes = Double.parseDouble(lat.substring(2)) / 1;

            String l0ng = data.substring(99, 99 + 10);
            int lng_degrees = Integer.parseInt(l0ng.substring(0, 3));
            double lng_minutes = Double.parseDouble(l0ng.substring(3)) / 1;

            double  latValue = lat_degrees + lat_minutes / 60;
            double lngValue = lng_degrees + lng_minutes / 60;
//                double latValue = 14.5401793;
//                double lngValue = 121.0042753;

            //Decode status
            String status = data.substring(24, 24 + 8);
            int status_conversion = Integer.parseInt(status.substring(1, 2));
            String vehicle_status = getStatus(status_conversion, speed);

            //Decode direction
            String direction = data.substring(72, 72 + 3);
            int direction_value = Integer.parseInt(direction);
            direction = String.valueOf(getDirection(direction_value));

            //Decode distance_km
            String distance_km = data.substring(82, 82 + 7);
            int distance_km_value = Integer.parseInt(distance_km);
            distance_km = String.valueOf(getDistance(distance_km_value));



            //Decode Time and Date
            String dateTimeString = data.substring(34, 34 + 2) + data.substring(32, 32 + 2) +
                    data.substring(36, 36 + 2) + data.substring(38, 38 + 2) +
                    data.substring(40, 40 + 2) + data.substring(42, 42 + 2);
            int year = Integer.parseInt(dateTimeString.substring(2, 4));
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
            java.sql.Timestamp timestamp = new java.sql.Timestamp(calendar.getTimeInMillis());

            int latInt = (int) latValue;
            int lngInt = (int) lngValue;

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

            String selectIdQuery = "SELECT id FROM tbl_allroads WHERE coords LIKE ?";
            List<Map<String, Object>> idResult = jdbcTemplate.queryForList(selectIdQuery, "%" + nearestCoordinate + "%");

            String road = idResult.stream()
                    .findFirst()
                    .map(row -> (int) row.get("id"))
                    .map(id -> {
                        String selectRoadsQuery = "SELECT roads FROM tbl_allroads WHERE id = ?";
                        List<Map<String, Object>> roadsResult = jdbcTemplate.queryForList(selectRoadsQuery, id);
                        return roadsResult.stream()
                                .map(row1 -> (String) row1.get("roads"))
                                .findFirst()
                                .orElse("");
                    })
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

            String combinedAddress = "";

            // Check if address is not empty
            if (!address.isEmpty()) {
                combinedAddress += address + ",";
            }

            // Check if road is not empty
            if (!road.isEmpty()) {
                combinedAddress += road + ",";
            }

            combinedAddress += locationFound;
            String test = "1";
            System.out.println("==========================================");
            System.out.println("IMEI :" + imei);
            System.out.println("Lat :" + latValue);
            System.out.println("Long :" + lngValue);
            System.out.println("Timestamp :" + timestamp);
            System.out.println("Status :" + vehicle_status);
            System.out.println("Direction :" + direction);
            System.out.println("Distance km :" + distance_km);
            System.out.println("Speed :" + speed);
            System.out.println("Fuel value :" + fuelValue);
            System.out.println("Color state :" + colorState);
            System.out.println("Tire preassure :" + tirePreassure);
            System.out.println("Temp sensor :" + tempSensor);
            System.out.println("Brand :" + brand);
            System.out.println("Model :" + model);
            System.out.println("GSM :" + gsm);
            System.out.println("Address :" + combinedAddress);
            System.out.println("==========================================");

            String dataCombination = imei + ";" + latValue + ";" + lngValue + ";" + timestamp + ";" + vehicle_status + ";" + direction + ";" + distance_km + ";" + speed + ";"
                    + fuelValue + ";" + colorState + ";" + tirePreassure + ";" + tempSensor + ";" + brand + ";" + model + ";" + gsm + ";" + combinedAddress;
            return dataCombination;
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
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

    private String getStatus(int status_conversion, String speed) {
        if (status_conversion == 0) {
            return "stopped";
        } else if (status_conversion == 1 && speed.equals("0")) {
            return "idling";
        } else if (status_conversion == 1 && Integer.parseInt(speed) > 0) {
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
    private void saveDataToDatabase(String data) {
        String query = "INSERT INTO tbl_vehicle_logs (imei,lat,`long`,date_time,`status`,direction,distance,speed,fuel_value,color_state,tire_preassure,temp_sensor,brand,model,gsm_signal,address) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String[] values = data.split(";");

        try {
            jdbcTemplate.update(query,
                    values[0],  // imei
                    values[1],  // lat
                    values[2],  // long
                    values[3],  // date_time
                    values[4],  // status
                    values[5],  // direction
                    values[6],  // distance
                    values[7],  // speed
                    values[8],  // fuel_value
                    values[9],  // color_state
                    values[10], // tire_preassure
                    values[11], // temp_sensor
                    values[12], // brand
                    values[13], // model
                    values[14], // gsm_signal
                    values[15]);// address
            String updateVehiclesQuery = "UPDATE tbl_vehicles SET vl_id = (SELECT MAX(id) FROM tbl_vehicle_logs WHERE imei = ?) WHERE imei = ?";
            jdbcTemplate.update(updateVehiclesQuery, values[0], values[0]);
        } catch (Exception e) {
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
