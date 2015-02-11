package com.github.thorqin.importcsv;

import com.github.thorqin.toolkit.db.DBService;
import com.github.thorqin.toolkit.utility.ConfigManager;
import com.github.thorqin.toolkit.utility.Serializer;
import com.github.thorqin.toolkit.validation.ValidateException;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nuo.qin on 1/23/2015.
 */
public class Import {

    private static File getJarPath() throws URISyntaxException {
        return new File(Import.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI().getPath());
    }

    private static String parseHead(String str) {
        if (str == null || str.isEmpty()) {
            return "null";
        } else
            return str;
    }
    private static String parseValue(String str) {
        if (str == null || str.isEmpty()) {
            return "null";
        } else
            return "'" + str.replaceAll("'", "''") + "'";
    }

    private static int executeSqlScript(DBService.DBSession session, File dir) throws IOException, SQLException {
        int count = 0;
        File directory = new File(dir, "sql");
        if (!directory.exists() || !directory.isDirectory())
            return 0;
        File file[] = directory.listFiles();
        for (int i = 0; i < file.length; i++) {
            if (!file[i].getName().matches("(?i).+\\.sql$"))
                continue;
            String textContent = Serializer.loadTextFile(file[i]);
            session.execute(textContent);
            session.commit();
            count++;
        }
        return count;
    }

    public static String[] readLine(Reader reader) throws IOException {
        List<String> result = new LinkedList<>();
        int state = 0;
        StringBuilder buffer = new StringBuilder();
        int value;
        boolean newLine = false;
        while ((value = reader.read()) != -1) {
            char ch = (char)value;
            if (state == 0 || state == 4) {
                if (ch == '\n') {
                    newLine = true;
                    break;
                } else if (ch == ',') {
                    result.add("");
                    state = 4;
                } else if (ch == '"') {
                    state = 1;
                } else {
                    buffer.append(ch);
                    state = 2;
                }
            } else if (state == 1) { // in "..."
                if (ch == '"') {
                    state = 3;
                } else {
                    buffer.append(ch);
                }
            } else if (state == 2) {
                if (ch == '\n') {
                    break;
                } else if (ch == ',') {
                    result.add(buffer.toString());
                    buffer = new StringBuilder();
                    state = 4;
                } else {
                    buffer.append(ch);
                }
            } else if (state == 3) {
                if (ch == '\n') {
                    break;
                } else if (ch == '"') {
                    buffer.append(ch);
                    state = 1;
                } else if (ch == ',') {
                    result.add(buffer.toString());
                    buffer = new StringBuilder();
                    state = 4;
                } else {
                    buffer.append(ch);
                    state = 2;
                }
            }
        }
        if (state == 0) {
            if (!newLine) {
                return null;
            }
        } else if (state == 4) {
            result.add("");
        } else {
            result.add(buffer.toString());
        }
        return result.toArray(new String[result.size()]);
    }

    private static int importCSV(DBService.DBSession session, File dir) throws IOException, SQLException {
        int importedFileCount = 0;
        File directory = new File(dir, "csv");
        if (!directory.exists() || !directory.isDirectory())
            return 0;
        File file[] = directory.listFiles();
        for (int i = 0; i < file.length; i++) {
            if (!file[i].getName().matches("(?i).+\\.csv$"))
                continue;
            System.out.print("Import file: " + file[i].getName() + ".");
            int lineCount = 0;
            try {
                String tableName = file[i].getName().split("\\.")[1];
                String textContent = Serializer.loadTextFile(file[i]);
                StringReader reader = new StringReader(textContent);
                BufferedReader br = new BufferedReader(reader);

                StringBuilder sb = new StringBuilder();
                sb.append("insert into ").append(tableName);
                String initSql = "";
                boolean isHead = true;
                int columnCount = 0;
                String[] line;
                while ((line = readLine(br)) != null) {
                    if (isHead) {
                        sb.append("(");
                        columnCount = line.length;
                        for (int j = 0; j < line.length; j++) {
                            if (j > 0)
                                sb.append(",");
                            sb.append(parseHead(line[j]));
                        }
                        sb.append(")");
                        initSql = sb.toString();
                        isHead = false;
                    } else {
                        lineCount++;
                        sb = new StringBuilder();
                        sb.append(initSql);
                        sb.append(" values (");
                        int j = 0;
                        for (; j < line.length; j++) {
                            if (j > 0)
                                sb.append(",");
                            sb.append(parseValue(line[j]));
                        }
                        for (; j < columnCount; j++) {
                            if (j > 0)
                                sb.append(",");
                            sb.append("null");
                        }
                        sb.append(");");

                        session.execute(sb.toString());
                        if (lineCount % 1000 == 0) {
                            session.commit();
                            System.out.print(".");
                        }
                    }
                }
                if (lineCount % 1000 != 0) {
                    session.commit();
                    System.out.print(".");
                }

                System.out.println("Success! Total count: " + lineCount);
            } catch (Exception ex) {
                System.out.println("Failed at row " + lineCount + "!");
                System.out.flush();
                ex.printStackTrace();
            }
            importedFileCount++;
        }
        return importedFileCount;
    }

    public static void main(String args[]) {
        try {
            System.out.println("Initialize ...");
            String configFilePath = getJarPath().getParentFile().toString();
            if (configFilePath.endsWith("/") || configFilePath.endsWith("\\"))
                configFilePath += "config.json";
            else
                configFilePath += "/config.json";
            File configFile = new File(configFilePath);
            if (!configFile.exists())
                throw new IOException("Can not load configuration file.");

            long beginTime = System.currentTimeMillis();

            ConfigManager configManager = new ConfigManager();
            configManager.loadFile(configFile);
            DBService.DBSetting dbSetting = configManager.get("db", DBService.DBSetting.class);
            DBService dbService = new DBService(dbSetting);

            int scriptCount = 0, csvCount = 0;
            try (DBService.DBSession session = dbService.getSession()) {
                session.setAutoCommit(false);
                String curDir = System.getProperty("user.dir");
                File directory = new File(curDir);
                System.out.println("Create Database Table ...");
                System.out.flush();
                scriptCount = executeSqlScript(session, directory);
                System.out.println("Import data ...");
                System.out.flush();
                csvCount = importCSV(session, directory);
            }
            long endTime = System.currentTimeMillis();
            long useMS = endTime - beginTime;
            String timeStr = ((double)useMS / 1000) + "s (≈" + (Math.round((double)useMS / 60 / 1000)) + " minutes)";

            System.out.println("Import Finished!");
            System.out.println("--------------------------------");
            System.out.println("   SQL File Script: " + scriptCount);
            System.out.println("   CSV File count: " + csvCount);
            System.out.println("--------------------------------");
            System.out.println("Total Used Time: " + timeStr + "\n");
        } catch (Exception e) {
            System.out.println("Finished with error!");
            System.err.println("Import failed: " + e.getMessage());
            System.err.println("----------------------------------");
            e.printStackTrace();
        }
    }
}
