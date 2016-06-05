/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cyberiantiger.minecraft.sqlconverter;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 *
 * @author antony
 */
public class Main {
    private static enum UUIDFormat {
        JAVA {
            @Override
            public UUID get(ResultSet rs, int column) throws SQLException {
                String uuidString = rs.getString(column);
                if (rs.wasNull()) return null;
                return UUID.fromString(uuidString);
            }
            @Override
            public void set(PreparedStatement ps, int column, UUID uuid) throws SQLException {
                ps.setString(column, uuid == null ? null : uuid.toString());
            }
            @Override
            public void update(ResultSet rs, int column, UUID uuid) throws SQLException {
                rs.updateString(column, uuid == null ? null : uuid.toString());
            }
        },
        NUMERIC {
            @Override
            public UUID get(ResultSet rs, int column) throws SQLException {
                BigInteger uuid = rs.getBigDecimal(column).toBigInteger();
                if (rs.wasNull()) return null;
                long lsw = uuid.longValue();
                long msw = uuid.shiftRight(64).longValue();
                return new UUID(msw, lsw);
            }
            @Override
            public void set(PreparedStatement ps, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    ps.setBigDecimal(column, null);
                } else {
                    BigInteger intuuid = BigInteger.valueOf(uuid.getMostSignificantBits());
                    intuuid = intuuid.shiftLeft(64);
                    intuuid = intuuid.add(BigInteger.valueOf(uuid.getLeastSignificantBits()));
                    ps.setBigDecimal(column, new BigDecimal(intuuid));
                }
            }
            @Override
            public void update(ResultSet rs, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    rs.updateBigDecimal(column, null);
                } else {
                    BigInteger intuuid = BigInteger.valueOf(uuid.getMostSignificantBits());
                    intuuid = intuuid.shiftLeft(64);
                    intuuid = intuuid.add(BigInteger.valueOf(uuid.getLeastSignificantBits()));
                    rs.updateBigDecimal(column, new BigDecimal(intuuid));
                }
            }
        },
        HEXADECIMAL {
            @Override
            public UUID get(ResultSet rs, int column) throws SQLException {
                String uuidString = rs.getString(column);
                if (rs.wasNull()) {
                    return null;
                }
                BigInteger uuidInt = new BigInteger(uuidString, 16);
                long lsw = uuidInt.longValue();
                long msw = uuidInt.shiftRight(64).longValue();
                return new UUID(msw, lsw);
            }
            @Override
            public void set(PreparedStatement ps, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    ps.setString(column, null);
                } else {
                    String uuidString = uuid.toString().replace("-", "");
                    ps.setString(column, uuidString);
                }
            }
            @Override
            public void update(ResultSet rs, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    rs.updateString(column, null);
                } else {
                    String uuidString = uuid.toString().replace("-", "");
                    rs.updateString(column, uuidString);
                }
            }
        },
        MOJANG {
            @Override
            public UUID get(ResultSet rs, int column) throws SQLException {
                String uuidString = rs.getString(column);
                if (rs.wasNull()) {
                    return null;
                }
                BigInteger uuidInt = new BigInteger(uuidString, 16);
                long lsw = uuidInt.longValue();
                long msw = uuidInt.shiftRight(64).longValue();
                return new UUID(msw, lsw);
            }
            @Override
            public void set(PreparedStatement ps, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    ps.setString(column, null);
                } else {
                    BigInteger intuuid = BigInteger.valueOf(uuid.getMostSignificantBits());
                    intuuid = intuuid.shiftLeft(64);
                    intuuid = intuuid.add(BigInteger.valueOf(uuid.getLeastSignificantBits()));
                    ps.setString(column, intuuid.toString(16));
                }
            }
            @Override
            public void update(ResultSet rs, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    rs.updateString(column, null);
                } else {
                    BigInteger intuuid = BigInteger.valueOf(uuid.getMostSignificantBits());
                    intuuid = intuuid.shiftLeft(64);
                    intuuid = intuuid.add(BigInteger.valueOf(uuid.getLeastSignificantBits()));
                    rs.updateString(column, intuuid.toString(16));
                }
            }
        },
        BINARY {
            @Override
            public UUID get(ResultSet rs, int column) throws SQLException {
                byte[] data = rs.getBytes(column);
                if (rs.wasNull()) {
                    return null;
                }
                ByteBuffer buf = ByteBuffer.wrap(data);
                return new UUID(buf.getLong(), buf.getLong());
            }

            @Override
            public void set(PreparedStatement ps, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    ps.setBinaryStream(column, null);
                } else {
                    ByteBuffer data = ByteBuffer.wrap(new byte[16]);
                    data.putLong(uuid.getMostSignificantBits());
                    data.putLong(uuid.getLeastSignificantBits());
                    ps.setBytes(column, data.array());
                }
            }

            @Override
            public void update(ResultSet rs, int column, UUID uuid) throws SQLException {
                if (uuid == null) {
                    rs.updateBytes(column, null);
                } else {
                    ByteBuffer data = ByteBuffer.wrap(new byte[16]);
                    data.putLong(uuid.getMostSignificantBits());
                    data.putLong(uuid.getLeastSignificantBits());
                    rs.updateBytes(column, data.array());
                }
            }
        };

        public abstract UUID get(ResultSet rs, int column) throws SQLException;
        public abstract void set(PreparedStatement ps, int column, UUID uuid) throws SQLException;
        public abstract void update(ResultSet rs, int column, UUID uuid) throws SQLException;
    }
    private static final URL PROFILE_URL;
    static {
        try {
            PROFILE_URL = new URL("https://api.mojang.com/profiles/minecraft");
        } catch (MalformedURLException ex) {
            throw new IllegalStateException();
        }
    }
    // Maximum queries per 10 minutes.
    private static final int DEFAULT_RATE = 600;
    private static final Pattern VALID_USERNAME = Pattern.compile("[a-zA-Z0-9_]{2,16}");
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final int BATCH_SIZE = 100; // Mojang's code says don't do over 100 at once.
    private static final Gson gson = new Gson();

    private static long lastQuery = 0L;
    private static long queryDelay = queryDelay(DEFAULT_RATE);

    private static long queryDelay(int rate) {
        if (rate <= 0)
            return 0L;
        // Round up, not down.
        return 1L + (10L * 60L * 1000L) / rate;
    }

    private static void usage() {
        System.err.println("Usage: java -jar sql-converter.jar [-R <rate>] [-n] [-d] [-o] [-F] [-u <username>] [-p <password>] <database url> <table> <name column> <uuid column>");
        System.err.println("     -R <rate>     - Maximum number of queries per 10 minutes, default 600"); 
        System.err.println("     -n            - dry run, don't do anything.");
        System.err.println("     -d            - Delete rows for names with no valid UUID.");
        System.err.println("     -o            - Use offline UUIDs");
        System.err.println("     -F            - Update existing UUIDs, don't just replace NULL");
        System.err.println("     -V            - Don't skip rows with invalid UUIDS");
        System.err.println("     -e <char>     - Escape for table and column names, default: none");
        System.err.println("     -u <username> - Database username");
        System.err.println("     -p <password> - Database password");
        System.err.println("     -f <format>   - UUIDs use the specified format");
        System.err.println("                   java        - Java's built in UUID format including '-' (default)");
        System.err.println("                   numeric     - Use BigDecimal");
        System.err.println("                   hexadecimal - Hex format, like java's without -");
        System.err.println("                   mojang      - Hex format, like java's without -, and no leading zeros");
        System.err.println("                   binary      - Binary format");
    }


    public static void main(String[] args) throws Exception {
        String dbUrl = null;
        String table = null;
        String nameColumn = null;
        String uuidColumn = null;
        String username = null;
        String password = null;
        String escape = "";
        boolean parseFlags = true;
        boolean dryRun = false;
        boolean offline = false;
        boolean skipInvalid = true;
        UUIDFormat format = UUIDFormat.JAVA;
        boolean delete = false;
        boolean force = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (parseFlags && arg.length() > 0 && arg.charAt(0) == '-') {
                if ("--".equals(args)) {
                    parseFlags = false;
                } else if ("-n".equals(arg)) {
                    dryRun = true;
                } else if ("-d".equals(arg)) {
                    delete = true;
                } else if ("-o".equals(arg)) {
                    offline = true;
                } else if ("-F".equals(arg)) {
                    force = true;
                } else if ("-V".equals(arg)) {
                    skipInvalid = false;
                } else if ("-e".equals(arg)) {
                    i++;
                    if (args.length == i) {
                        System.err.println("missing argument for -e");
                        usage();
                        System.exit(1);
                    }
                    escape = args[i];
                } else if ("-u".equals(arg)) {
                    i++;
                    if (args.length == i) {
                        System.err.println("missing argument for -u");
                        usage();
                        System.exit(1);
                    }
                    username = args[i];
                } else if ("-p".equals(arg)) {
                    i++;
                    if (args.length == i) {
                        System.err.println("missing argument for -p");
                        usage();
                        System.exit(1);
                    }
                    password = args[i];
                } else if ("-f".equals(arg)) {
                    i++;
                    if (args.length == i) {
                        System.err.println("missing argument for -f");
                        usage();
                        System.exit(1);
                    }
                    try {
                        format = UUIDFormat.valueOf(args[i].toUpperCase());
                    } catch (IllegalArgumentException e) {
                        System.err.println("Unknown format: " + args[i]);
                        usage();
                        System.exit(1);
                    }
                } else if ("-R".equals(arg)) {
                    i++;
                    if (args.length == i)  {
                        System.err.println("missing argument for -R");
                        usage();
                        System.exit(1);
                    }
                    try {
                        queryDelay = queryDelay(Integer.parseInt(args[i]));
                    } catch (NumberFormatException e) {
                        System.err.println("rate must be an integer: " + args[i]);
                        usage();
                        System.exit(1);
                    }
                } else {
                    System.err.println("Unexpected flag: " + arg);
                    usage();
                    System.exit(1);
                }
                continue;
            }
            if (dbUrl == null) {
                dbUrl = arg;
            } else if (table == null) {
                table = arg;
            } else if (nameColumn == null) {
                nameColumn = arg;
            } else if (uuidColumn == null) {
                uuidColumn = arg;
            } else {
                System.err.println("Too many arguments");
                usage();
                System.exit(1);
            }
        }

        if (uuidColumn == null) {
            System.err.println("Too few arguments");
            usage();
            System.exit(0);
        }

        System.out.println("Connecting to database " + dbUrl + " with username = " + username + " and password = " + password);
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            conn.setAutoCommit(true);
            try {
                String queryAllSql = "select " + escape + nameColumn + escape + " from " + table;
                System.out.println("Selecting all player names from the database with: " + queryAllSql);
                Set<String> names = new HashSet<String>();
                PreparedStatement ps = conn.prepareStatement(queryAllSql);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (VALID_USERNAME.matcher(name).matches()) {
                        names.add(name);
                    }
                }
                rs.close();
                Map<String,UUID> uuidMap;
                if (offline) {
                    uuidMap = new HashMap<String, UUID>();
                    for (String name : names) {
                        uuidMap.put(name, getOfflineUUID(name));
                    }
                } else {
                    uuidMap = getOnlineUUIDs(names);
                }
                List<String> primaryKeys = new ArrayList<String>();
                DatabaseMetaData metaData = conn.getMetaData();
                rs = metaData.getPrimaryKeys(null, null, table);
                while (rs.next()) {
                    primaryKeys.add(rs.getString("COLUMN_NAME"));
                }
                rs.close();
                
                String updateAllSql = "select " + escape + nameColumn + escape + ", " + escape + uuidColumn + escape;
                for (String pkCol : primaryKeys) {
                    updateAllSql += ", " + escape + pkCol + escape;
                }
                updateAllSql += " from " + table;

                System.out.println("Updating the database with SQL: " + updateAllSql);
                PreparedStatement ps2 = conn.prepareStatement(updateAllSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                rs = ps2.executeQuery();
                while (rs.next()) {
                    String name = rs.getString(1);
                    UUID uuid = null;
                    try { 
                        uuid = format.get(rs, 2);
                    } catch (RuntimeException ex) {
                        if (skipInvalid) {
                            System.out.println("Skipping row with name = " + name + " uuid = " + rs.getObject(2) + "; Invalid uuid");
                            continue;
                        } else {
                            System.out.println("Ignoring invalid uuid: " + rs.getObject(2) + " for name: " + name);
                        }
                    }
                    if (!VALID_USERNAME.matcher(name).matches()) {
                        System.out.println("Skipping row with name = " + name + " uuid = " + uuid + "; Invalid name");
                        continue;
                    }
                    if (force || uuid == null) {
                        UUID newUuid = uuidMap.get(name);
                        if (newUuid != null) {
                            if (!newUuid.equals(uuid)) {
                                System.out.println("Updating row with name = " + name + " uuid = " + uuid + " to uuid = " + newUuid);
                                if (!dryRun) {
                                    format.update(rs, 2, newUuid);
                                }
                            }
                            rs.updateRow();
                        } else if (delete) {
                            System.out.println("Deleting row with name = " + name + " uuid = " + uuid);
                            if (!dryRun) {
                                rs.deleteRow();
                            }
                        }
                    }
                }
                rs.close();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean isOfflineUUID(String player, UUID uuid) {
        return getOfflineUUID(player).equals(uuid);
    }

    private static UUID getOfflineUUID(String player) {
        return UUID.nameUUIDFromBytes(("OfflinePlayer:" + player).getBytes(UTF8));
    }

    private static Map<String,UUID> getOnlineUUIDs(Collection<String> tmp) throws IOException {
        long now = System.currentTimeMillis();
        if (lastQuery + queryDelay > now) {
            try {
                Thread.sleep(lastQuery + queryDelay - now);
            } catch (InterruptedException ex) {
            }
            lastQuery += queryDelay;
        } else {
            lastQuery = now;
        }
        List<String> players = new ArrayList<String>(tmp);
        List<String> batch = new ArrayList<String>();
        Map<String,UUID> result = new HashMap<String,UUID>();
        while (!players.isEmpty()) {
            for (int i = 0; !players.isEmpty() && i < BATCH_SIZE; i++) {
                batch.add(players.remove(players.size()-1));
            }
            HttpURLConnection connection = (HttpURLConnection) PROFILE_URL.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type","application/json; encoding=UTF-8");
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            OutputStream out = connection.getOutputStream();
            out.write(gson.toJson(batch).getBytes(UTF8));
            out.close();
            Reader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            Profile[] profiles = gson.fromJson(in, Profile[].class);
            for (Profile profile : profiles) {
                result.put(profile.getName(), profile.getUUID());
            }
            batch.clear();
        }
        return result;
    }
}