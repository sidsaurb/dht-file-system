package com.cs425;

import com.google.gson.Gson;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Main {

    public static String IP = "localhost";
    public static int MASTER_PORT = 12345;
    static String FINGER_TABLE_DIRECTORY = "/home/siddhant/Documents/cs425/project/fingertables/";
    static String FILE_DIRECTORY = "/home/siddhant/Documents/cs425/project/files/";
    static String CURRENT_PATH = "/filesystem/";


    static Stack<String> directoryStack;


//    static int N = 65536;
//    static int logN = 16;

    static int N = 64;
    static int logN = 6;

    public static boolean alreadyListening = false;
    public static int myPort = 0;
    public static ServerSocket listener;


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        directoryStack = new Stack<>();
        while (true) {
            try {
                Thread.sleep(500);
            } catch (Exception ignored) {
            }
            System.out.print("> Enter command: ");
            String command = scanner.next();
            switch (command) {
                // join
                case "j":
                    if (!alreadyListening) {
//                        System.out.print("> Enter 5 digit port: ");
                        int port = scanner.nextInt();
                        int nodeHash = getHash(port);
                        alreadyListening = true;
                        listenToSocket(nodeHash, port);
                        myPort = port;
                        if (port == MASTER_PORT) {
                            initializeMasterPort(nodeHash);
                        } else {
                            initializeOtherPorts(port, nodeHash);
                        }
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
                // just listen without joining
                case "l":
                    if (!alreadyListening) {
//                        System.out.print("> Enter 5 digit port: ");
                        int port1 = scanner.nextInt();
                        int nodeHash1 = getHash(port1);
                        alreadyListening = true;
                        listenToSocket(nodeHash1, port1);
                        myPort = port1;
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
                // upload a file
                case "u":
                    if (alreadyListening) {
                        String filename1 = scanner.next();
                        if (!processUploadCommand(filename1)) {
                            System.out.println("File doesn't exist");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // download a file
                case "d":
                    if (alreadyListening) {
                        String filename2 = scanner.next();
                        if (!processDownloadCommand(filename2, "")) {
                            System.out.println("The file does not exists");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "pwd":
                    if (alreadyListening) {
                        System.out.println(CURRENT_PATH);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "ls":
                    if (alreadyListening) {
                        String contents = getDirectoryContents();
                        prettyPrint(contents);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "cd":
                    if (alreadyListening) {
                        String directoryName = scanner.next();
                        if (!processCdCommand(directoryName)) {
                            System.out.println("No such directory exists");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "mkdir":
                    if (alreadyListening) {
//                        System.out.print("> Enter directory name: ");
                        String directoryName = scanner.next();
                        if (processMkDirCommand(CURRENT_PATH + directoryName + "/")) {
                            System.out.println("Directory successfully created");
                        } else {
                            System.out.println("There was an error in creating the directory");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "rm":
                    if (alreadyListening) {
                        String fileName = scanner.next();
                        if (!processRmCommand(fileName)) {
                            System.out.println("File not found");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "rmdir":
                    if (alreadyListening) {
                        String directoryName = scanner.next();
                        if (!processRmDirCommand(directoryName)) {
                            System.out.println("Directory not found");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "cp":
                    if (alreadyListening) {
                        String currentFileRelativePath = scanner.next(); // relative path
                        String newFileRelativePath = scanner.next(); // relative path
                        if (processCpCommand(currentFileRelativePath, newFileRelativePath)) {
                            System.out.println("Copied successfully");
                        } else {
                            System.out.println("There was some error in copying");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "mv":
                    if (alreadyListening) {
                        String currentFileRelativePath = scanner.next(); // relative path
                        String newFileRelativePath = scanner.next(); // relative path
                        boolean success = processMvCommand(currentFileRelativePath, newFileRelativePath);
                        if (!success) {
                            System.out.println("There was an error in moving");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "pushd":
                    if (alreadyListening) {
                        String relativePath = scanner.next(); // relative path
                        boolean success = processPushdCommand(relativePath);
                        if (!success) {
                            System.out.println("There was an error in pushing");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "popd":
                    if (alreadyListening) {
                        if (directoryStack.size() == 0) {
                            System.out.println("Stack is empty");
                        } else {
                            CURRENT_PATH = directoryStack.pop();
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // quit the program
                case "q":
                    break;
                default:
                    System.out.println("Invalid command");
                    break;
            }
            if (command.equals("q")) {
                try {
                    System.out.println("Program exiting..");
                    listener.close();
                } catch (Exception e) {
//                    e.printStackTrace();
                }
                break;
            }
        }
    }

    private static boolean processPushdCommand(String relativePath) {
        directoryStack.push(CURRENT_PATH);
        String[] arguments = relativePath.split("/");
        for (String item1 : arguments) {
            if (!item1.equals("..")) {
                String contents = getDirectoryContents();
                String[] contentList = contents.split("\n");
                boolean exists = false;
                for (String item : contentList) {
                    String[] temp = item.split("\t");
                    if (temp[0].equals(item1) && temp[1].equals("d")) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    CURRENT_PATH = CURRENT_PATH + item1 + "/";
                } else {
                    return false;
                }
            } else {
                if (!CURRENT_PATH.equals("/filesystem/")) {
                    CURRENT_PATH = getDirectoryFromFilePath(CURRENT_PATH.substring(0, CURRENT_PATH.length() - 1));
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static void prettyPrint(String contents) {
        String[] list = contents.split("\n");
        Arrays.sort(list);
        int max = 0;
        for (String item : list) {
            if (item.length() > max) {
                max = item.length();
            }
        }
        for (String item : list) {
            if (!item.equals("")) {
                int length = item.length();
                String[] temp = item.split("\t");
                System.out.println(temp[0] + new String(new char[max - length]).replace("\0", " ") + "\t\t" + temp[1]);
            }
        }
    }

    private static boolean processMvCommand(String currentFileRelativePath, String newFileRelativePath) {
        String prev_cur_path = CURRENT_PATH;
        String currentFileDirectory = getDirectoryFromFilePath(currentFileRelativePath);
        String newFileDirectory = getDirectoryFromFilePath(newFileRelativePath);
        String filename = getBaseFile(currentFileRelativePath);
        String filename2 = getBaseFile(newFileRelativePath);
        String currentPathFirstCase, currentFileSecondCase;
        if (!currentFileDirectory.equals("")) {
            if (!processCdCommand(currentFileDirectory)) {
                return false;
            }
        }
        currentPathFirstCase = CURRENT_PATH;
        CURRENT_PATH = prev_cur_path;
        if (!newFileDirectory.equals("")) {
            if (!processCdCommand(newFileDirectory)) {
                return false;
            }
        }
        currentFileSecondCase = CURRENT_PATH;
        CURRENT_PATH = prev_cur_path;
        if (currentFileSecondCase.equals(currentPathFirstCase) && filename.equals(filename2)) {
            return true;
        }

        processCpCommand(currentFileRelativePath, newFileRelativePath);

        if (!currentFileDirectory.equals("")) {
            if (!processCdCommand(currentFileDirectory)) {
                return false;
            }
        }

        String contents = getDirectoryContents();
        String[] contentList = contents.split("\n");
        boolean isDirectory = false;
        for (String item : contentList) {
            String[] temp = item.split("\t");
            if (temp[0].equals(filename) && temp[1].equals("d")) {
                isDirectory = true;
                break;
            }
        }
        if (isDirectory) {
            processRmDirCommand(filename);
        } else {
            processRmCommand(filename);
        }
        CURRENT_PATH = prev_cur_path;
        return true;
    }

    private static boolean processDownloadCommand(String fileToBeDownloaded, String destinationFile) {
        if (destinationFile.equals("")) {
            int fileHash2 = getSHA1Hash(CURRENT_PATH + fileToBeDownloaded);
            String filePort2 = getFilePort(fileHash2, CURRENT_PATH + fileToBeDownloaded);
            if (filePort2.equals("null")) {
                return false;
            } else {
                downloadFile(filePort2, fileToBeDownloaded);
                System.out.println("Downloaded from port: " + filePort2);
            }
            return true;
        } else {
            int fileHash2 = getSHA1Hash(CURRENT_PATH + fileToBeDownloaded);
            String filePort2 = getFilePort(fileHash2, CURRENT_PATH + fileToBeDownloaded);
            if (filePort2.equals("null")) {
                return false;
            } else {
                String destFileBaseName = getBaseFile(destinationFile);
                downloadFileToUploadFolder(filePort2, fileToBeDownloaded, destFileBaseName);
//                System.out.println("Downloaded from port: " + filePort2);
            }
            return true;
        }
    }


    private static boolean processCpCommand(String currentFileRelativePath, String newFileRelativePath) {
//        System.out.println(currentFileRelativePath + "\t" + newFileRelativePath);
        String currentFileDirectory = getDirectoryFromFilePath(currentFileRelativePath);
        String newFileDirectory = getDirectoryFromFilePath(newFileRelativePath);
        String prev_cur_path = CURRENT_PATH;
        if (!currentFileDirectory.equals("")) {
            if (!processCdCommand(currentFileDirectory)) {
                return false;
            }
        }
        String curFileBaseName = getBaseFile(currentFileRelativePath);
        String newFileBaseName = getBaseFile(newFileRelativePath);


        String contents = getDirectoryContents();
        String[] contentList = contents.split("\n");
        boolean isDirectory = false;
        for (String item : contentList) {
            String[] temp = item.split("\t");
            if (temp[0].equals(curFileBaseName) && temp[1].equals("d")) {
                isDirectory = true;
                break;
            }
        }

        if (!isDirectory) {
            int curFileHash = getSHA1Hash(CURRENT_PATH + curFileBaseName);
            String curFilePort = getFilePort(curFileHash, CURRENT_PATH + curFileBaseName);
            downloadFileToUploadFolder(curFilePort, curFileBaseName, newFileBaseName);
            CURRENT_PATH = prev_cur_path;
            if (!newFileDirectory.equals("")) {
                if (!processCdCommand(newFileDirectory)) {
                    return false;
                }
            }
            processUploadCommand(newFileBaseName);
            CURRENT_PATH = prev_cur_path;
            return true;
        } else {
            CURRENT_PATH = prev_cur_path;
            if (!newFileDirectory.equals("")) {
                processCdCommand(newFileDirectory);
            }
            processMkDirCommand(CURRENT_PATH + newFileBaseName + "/");
            CURRENT_PATH = prev_cur_path;
            processCdCommand(currentFileRelativePath);
            String contents1 = getDirectoryContents();
            String[] contentList1 = contents1.split("\n");
            CURRENT_PATH = prev_cur_path;
            for (String item : contentList1) {
                if (!item.equals("")) {
                    String[] temp = item.split("\t");
                    processCpCommand(currentFileRelativePath + "/" + temp[0], newFileRelativePath + "/" + temp[0]);
                }
            }
        }
        return true;
    }

    private static boolean processUploadCommand(String filename) {
        File f = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + filename);
        if (f.exists() && !f.isDirectory()) {
            int pathHash1 = getSHA1Hash(CURRENT_PATH + filename);
            uploadFile(CURRENT_PATH + filename, pathHash1);
            return true;
        } else {
            return false;
        }
    }

    public static boolean isFile(String relativePath) {
        String fileDirectory = getDirectoryFromFilePath(relativePath);
        String prev_cur_path = CURRENT_PATH;

        String fileBaseName = getBaseFile(relativePath);

        if (!fileDirectory.equals("")) {
            if (!processCdCommand(fileDirectory)) {
                System.out.println("File/Directory does not exist");
            }
        }

        String contents = getDirectoryContents();
        String[] contentList = contents.split("\n");
        boolean isDirectory = false;
        for (String item : contentList) {
            String[] temp = item.split("\t");
            if (temp[0].equals(fileBaseName) && temp[1].equals("d")) {
                isDirectory = true;
                break;
            }
        }
        CURRENT_PATH = prev_cur_path;
        return !isDirectory;
    }

    private static boolean processCdCommand(String directoryName) {
        String oldPath = CURRENT_PATH;
        String[] arguments = directoryName.split("/");
        for (String item1 : arguments) {
            if (!item1.equals("..")) {
                String contents = getDirectoryContents();
                String[] contentList = contents.split("\n");
                boolean exists = false;
                for (String item : contentList) {
                    String[] temp = item.split("\t");
                    if (temp[0].equals(item1) && temp[1].equals("d")) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    CURRENT_PATH = CURRENT_PATH + item1 + "/";
                } else {
                    return false;
                }
            } else {
                if (!CURRENT_PATH.equals("/filesystem/")) {
                    CURRENT_PATH = getDirectoryFromFilePath(CURRENT_PATH.substring(0, CURRENT_PATH.length() - 1));
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean processRmDirCommand(String directoryRelativePath) {
        if (isFile(directoryRelativePath)) {
            System.out.println(directoryRelativePath + " is not a directory");
            // only because of printing
            return true;
        }

        String currentFileDirectory = getDirectoryFromFilePath(directoryRelativePath);
        String prev_cur_path = CURRENT_PATH;
        if (!currentFileDirectory.equals("")) {
            if (!processCdCommand(currentFileDirectory)) {
                return false;
            }
        }
        String directoryName = getBaseFile(directoryRelativePath);


        String directoryPath = CURRENT_PATH + directoryName + "/";
        int directoryPathHash = getSHA1Hash(directoryPath);
        String directoryPathPort = getSuccessorPort(String.valueOf(directoryPathHash));
        String command = "remove_directory\t" + directoryPath + "\n";
        sendCommand(directoryPathPort, command);

        int currentDirectoryHash = getSHA1Hash(CURRENT_PATH);
        String currentDirectoryPort = getSuccessorPort(String.valueOf(currentDirectoryHash));
        command = "delete_directory_entry\t" + String.valueOf(currentDirectoryHash) + "\t"
                + CURRENT_PATH + "\t" + directoryName + "\t" + "d" + "\n";
        sendCommand(currentDirectoryPort, command);

        CURRENT_PATH = prev_cur_path;
        return true;
//        System.out.println("Directory deleted successfully");
    }

    private static boolean processRmCommand(String fileRelativePath) {
        if (!isFile(fileRelativePath)) {
            System.out.println(fileRelativePath + " is not a file");
            // only because of printing
            return true;
        }
        String currentFileDirectory = getDirectoryFromFilePath(fileRelativePath);
        String prev_cur_path = CURRENT_PATH;
        if (!currentFileDirectory.equals("")) {
            if (!processCdCommand(currentFileDirectory)) {
                return false;
            }
        }
        String curFileBaseName = getBaseFile(fileRelativePath);

        String command;
        String pathname = CURRENT_PATH + curFileBaseName;
        int pathHash = getSHA1Hash(pathname);
        String filePort = getSuccessorPort(String.valueOf(pathHash));

        String actualFilePort = getFilePort(pathHash, pathname);
        command = "delete_file_from_upload_folder\t" + curFileBaseName + "\n";
        sendCommand(actualFilePort, command);

        command = "remove_document\t" + String.valueOf(pathHash) + "\t" + pathname + "\n";
        String response = sendCommandWithReturnValue(filePort, command);

        if (response.equals("success")) {
            String directoryPath = getDirectoryFromFilePath(pathname);
            int directoryPathHash = getSHA1Hash(directoryPath);
            String directoryPathPort = getSuccessorPort(String.valueOf(directoryPathHash));
            command = "delete_directory_entry\t" + String.valueOf(directoryPathHash) + "\t"
                    + directoryPath + "\t" + curFileBaseName + "\t" + "f" + "\n";
            sendCommand(directoryPathPort, command);
//            System.out.println("File deleted successfully");
            CURRENT_PATH = prev_cur_path;
            return true;
        } else {
            CURRENT_PATH = prev_cur_path;
            return false;
        }
    }


    private static boolean processMkDirCommand(String directoryPath) {
        try {
            int directoryHash = getSHA1Hash(directoryPath);
            // portOfDirectory is the final node which has this directory as its responsibility
            String portOfDirectory = getSuccessorPort(String.valueOf(directoryHash));
            boolean success;
            // if my responsibility
            if (Integer.parseInt(portOfDirectory) == myPort) {
                NodeResponsibilityTable myTable = getResponsibilityTable(portOfDirectory);
                success = myTable.createDirectory(directoryHash, directoryPath);
                if (success) {
                    writeToFileAsJson(portOfDirectory + "_files", new Gson().toJson(myTable));
                }
            } else {
                // if not my responsibility then forward command to the successor port
                // but first check if file with same key already exists or not by
                // sending a filename command to the successor node

                Socket socket = new Socket(IP, Integer.parseInt(portOfDirectory));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String toWrite = "mkdir\t" + String.valueOf(directoryHash) + "\t" + directoryPath + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String response = br.readLine();
                socket.close();
                if (response.equals("success")) {
                    success = true;
                } else {
                    success = false;
                }
            }
            if (success) {
                String parentDirectoryPath = getDirectoryFromFilePath(directoryPath.substring(0, directoryPath.length() - 1));
                int parentDirectoryHash = getSHA1Hash(parentDirectoryPath);
                String parentDirectoryPort = getSuccessorPort(String.valueOf(parentDirectoryHash));
                String temp = directoryPath.substring(0, directoryPath.length() - 1);
                String directoryName = temp.substring(temp.lastIndexOf("/") + 1);
                if (!parentDirectoryPort.equals(String.valueOf(myPort))) {
                    String command1 = "update_directory_contents" + "\t" + parentDirectoryPath + "\t" + directoryName + "\t" + "d" + "\n";
                    sendCommand(parentDirectoryPort, command1);
                } else {
                    NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                    myTable.updateDirectoryEntry(parentDirectoryHash, parentDirectoryPath, directoryName, "d");
                    writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                }
            }
            return success;
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return false;
    }

    private static String getDirectoryContents() {
        try {
            int directoryHash = getSHA1Hash(CURRENT_PATH);
            String portOfFile = getSuccessorPort(String.valueOf(directoryHash));
            Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "get_directory_contents\t" + CURRENT_PATH + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String response = "";
            String line;
            do {
                line = br.readLine();
                if (line != null) {
                    response += line + "\n";
                }
            } while (line != null);
            socket.close();
            return response.trim();
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return "";
    }

    // takes port which has the file and downloads the file from it
    private static void downloadFile(String filePort, String filename) {
        try {
            Socket socket = new Socket(IP, Integer.parseInt(filePort));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "downloadfile\t" + String.valueOf(filename) + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStream in = socket.getInputStream();
            String filepath = FILE_DIRECTORY + String.valueOf(myPort) + "/download/" + filename;
            OutputStream out = new FileOutputStream(filepath);
            copy(in, out);
            out.close();
            in.close();
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
    }

    private static void downloadFileToUploadFolder(String filePort, String fileToBeDownloaded, String destinationFile) {
        try {
            Socket socket = new Socket(IP, Integer.parseInt(filePort));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "downloadfile\t" + String.valueOf(fileToBeDownloaded) + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStream in = socket.getInputStream();
            String filepath = FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + destinationFile;
            OutputStream out = new FileOutputStream(filepath);
            copy(in, out);
            out.close();
            in.close();
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
    }

    // copies the streams
    static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        int len;
        while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
    }

    private static void uploadFile(String pathName, int pathHash) {
        try {
            // portOfFile is the final node which has this file as its responsibility
            String portOfFile = getSuccessorPort(String.valueOf(pathHash));
            // if my responsibility
            if (Integer.parseInt(portOfFile) == myPort) {
                NodeResponsibilityTable myTable = getResponsibilityTable(portOfFile);
                String res = myTable.createFile(pathHash, pathName, String.valueOf(myPort));
                writeToFileAsJson(portOfFile + "_files", new Gson().toJson(myTable));

                String getOriginalFilePort = getFilePort(pathHash, pathName);

                // Delete old file
                if (!res.equals("") && !res.equals(getOriginalFilePort)) {
                    String command = "delete_file_from_upload_folder\t" + getBaseFile(pathName) + "\n";
                    sendCommand(res, command);
                }
//                System.out.println("File uploaded");

            } else {
                // if not my responsibility then forward command to the successor port
                Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String toWrite = "file_upload\t" + String.valueOf(pathHash) + "\t" + String.valueOf(myPort) + "\t" + pathName + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String response = br.readLine();
                socket.close();
                if (response.equals("success")) {
//                    System.out.println("File uploaded");
                } else {
                    System.out.println("File with the same key already exists");
                }
            }

            // update directory entry of the directory containing this file
            String directoryPath = getDirectoryFromFilePath(pathName);
            int directoryHash = getSHA1Hash(directoryPath);
            String directoryPort = getSuccessorPort(String.valueOf(directoryHash));
            String filename = pathName.substring(pathName.lastIndexOf("/") + 1);
            if (!directoryPort.equals(String.valueOf(myPort))) {
                String command1 = "update_directory_contents" + "\t" + directoryPath + "\t" + filename + "\tf" + "\n";
                sendCommand(directoryPort, command1);
            } else {
                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                myTable.updateDirectoryEntry(directoryHash, directoryPath, filename, "f");
                writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
            }

        } catch (Exception e) {
//            e.printStackTrace();
        }
    }

    // returns port where the file actually resides
    private static String getFilePort(int pathHash, String filePath) {
        try {
//            String portOfFile = getSuccessorPort(String.valueOf(getHash(myPort)), String.valueOf(fileHash));
            String portOfFile = getSuccessorPort(String.valueOf(pathHash));
            if (Integer.parseInt(portOfFile) == myPort) {
                String fileDestinationPort = getFileDestinationPort(portOfFile, pathHash, filePath);
                if (fileDestinationPort == null) {
                    return "null";
                } else {
                    return fileDestinationPort;
                }
            } else {
                Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String toWrite = "filename\t" + String.valueOf(pathHash) + "\t" + filePath + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String fileDestinationPort = br.readLine();
                socket.close();
                if (fileDestinationPort.equals("null")) {
                    return "null";
                } else {
                    return fileDestinationPort;
                }
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return "null";
    }

    // function to actually read file responsibility table and search for destination port of a file
    private static String getFileDestinationPort(String portOfFile, int pathHash, String filePath) {
        NodeResponsibilityTable myTable = getResponsibilityTable(portOfFile);
        NodeResponsibilityTableEntry entry = myTable.entries.get(pathHash);
        if (entry.entry.get(filePath) == null) {
            return null;
        } else {
            return entry.entry.get(filePath).fileLink;
        }
    }

    private static NodeResponsibilityTable getResponsibilityTable(String portOfFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + portOfFile + "_files"))) {
            String line;
            if ((line = reader.readLine()) != null) {
                return new Gson().fromJson(line, NodeResponsibilityTable.class);
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return new NodeResponsibilityTable();
    }

    private static void initializeOtherPorts(int port, int nodeHash) {
        try {
            // query master node for successor of this newly created node
            Socket socket = new Socket(IP, MASTER_PORT);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "successor\t" + String.valueOf(nodeHash) + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String successorNodePort = br.readLine();
            String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
            socket.close();

            if (successorNodeHash.equals(String.valueOf(getHash(port)))) {
                System.out.println("Node with the same key already exists.. try another value");
            } else {

                // send a command to its successor node to update its file responsibility
                // the successor node will update its responsibility and reply back with
                // this new node's responsibility
                socket = new Socket(IP, Integer.valueOf(successorNodePort));
                dos = new DataOutputStream(socket.getOutputStream());
                toWrite = "UpdateFileList\t" + String.valueOf(nodeHash) + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                isr = new InputStreamReader(socket.getInputStream());
                br = new BufferedReader(isr);
                String receivedData = br.readLine();
                socket.close();
                writeToFileAsJson(String.valueOf(port) + "_files", receivedData);

                // send a command to its successor node to update its finger table
                // the successor node will in-turn forward this to its own successor till
                // this new node is reached again. After hitting back again this node
                // will create it's finger table
                socket = new Socket(IP, Integer.valueOf(successorNodePort));
                dos = new DataOutputStream(socket.getOutputStream());
                toWrite = "UpdateFingerTables\t" + String.valueOf(nodeHash) + "\t" + String.valueOf(port)
                        + "\t" + successorNodeHash + "\t" + successorNodePort + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                socket.close();

                createLocalDirectories();
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
    }

    private static void initializeMasterPort(int nodeHash) {
        ArrayList<String> contents = new ArrayList<>();
        for (int i = 1; i <= logN; i++) {
            int temp = (nodeHash + (int) Math.pow(2, i - 1)) % N;
            contents.add(String.valueOf(temp) + "\t" + nodeHash + "\t" + String.valueOf(MASTER_PORT));
        }
        writeToFile(String.valueOf(MASTER_PORT), contents);

        NodeResponsibilityTable myTable = new NodeResponsibilityTable();
        for (int i = 0; i < N; i++) {
            myTable.entries.put(i, new NodeResponsibilityTableEntry());
        }
        myTable.min = (nodeHash + 1) % N;

        int initialDirectoryHash = getSHA1Hash(CURRENT_PATH);
        if (!myTable.createDirectory(initialDirectoryHash, CURRENT_PATH)) {
            System.out.println("Error creating initial directory");
        }

        writeToFileAsJson(String.valueOf(MASTER_PORT) + "_files", new Gson().toJson(myTable));
        createLocalDirectories();

    }

    private static void createLocalDirectories() {
        File theDir = new File(FILE_DIRECTORY + String.valueOf(myPort));
        if (!theDir.exists()) {
            try {
                theDir.mkdir();
            } catch (SecurityException se) {
            }
        }
        File theDir1 = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload");
        if (!theDir1.exists()) {
            try {
                theDir1.mkdir();
            } catch (SecurityException se) {
            }
        }
        File theDir2 = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/download");
        if (!theDir2.exists()) {
            try {
                theDir2.mkdir();
            } catch (SecurityException se) {
            }
        }
    }

    private static void listenToSocket(final int hash, final int port) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("> Listening on port " + String.valueOf(port));
                    listener = new ServerSocket(port);
                    listener.setReuseAddress(true);
                    try {
                        while (true) {
                            Socket socket = listener.accept();
                            OutputStream os = socket.getOutputStream();
                            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                            BufferedReader br = new BufferedReader(isr);
                            String command = br.readLine();
                            if (isFirstWord(command, "successor")) {
                                String targetNode = command.split("\t")[1];
                                String a = getSuccessorPort(targetNode);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(a + "\n");
                                dos.flush();
                            } else if (isFirstWord(command, "UpdateFingerTables")) {
                                String[] temp = command.split("\t");
                                String nodeHash = temp[1];
                                String nodePort = temp[2];
                                String succHash = temp[3];
                                String succPort = temp[4];
                                UpdateFingerTableAndForwardCommand(command, port, hash, nodeHash, nodePort, succHash, succPort);
                            } else if (isFirstWord(command, "UpdateFileList")) {
                                String[] temp = command.split("\t");
                                String dataToSend = updateFileResponsibility(String.valueOf(port), temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (isFirstWord(command, "filename")) {
                                // name is misleading. actually query for port number of a file.
                                String[] temp = command.split("\t");
                                String dataToSend = getFileDestinationPort(String.valueOf(port), Integer.parseInt(temp[1]), temp[2]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (isFirstWord(command, "file_upload")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(port));
                                String res = myTable.createFile(Integer.parseInt(temp[1]), temp[3], temp[2]);
                                writeToFileAsJson(String.valueOf(port) + "_files", new Gson().toJson(myTable));
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes("success" + "\n");
                                dos.flush();

                                String getOriginalFilePort = getFilePort(Integer.parseInt(temp[1]), temp[3]);
                                // Delete old file
                                if (!res.equals("") && !res.equals(String.valueOf(getOriginalFilePort))) {
                                    command = "delete_file_from_upload_folder\t" + getBaseFile(temp[3]) + "\n";
                                    sendCommand(res, command);
                                }

                            } else if (isFirstWord(command, "downloadfile")) {
                                String[] temp = command.split("\t");
                                InputStream in = new FileInputStream(FILE_DIRECTORY + String.valueOf(port) + "/upload/" + temp[1]);
                                copy(in, os);
                                os.close();
                                in.close();
                            } else if (isFirstWord(command, "update_directory_contents")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                myTable.updateDirectoryEntry(getSHA1Hash(temp[1]), temp[1], temp[2], temp[3]);
                                writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                            } else if (isFirstWord(command, "get_directory_contents")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                int directoryHash = getSHA1Hash(temp[1]);
                                String contents = myTable.getContentsOfDirectory(directoryHash, temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (isFirstWord(command, "mkdir")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.createDirectory(Integer.parseInt(temp[1]), temp[2]);
                                if (success) {
                                    writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                                String contents = success ? "success" : "failure";
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (isFirstWord(command, "remove_document")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.deleteDocument(Integer.parseInt(temp[1]), temp[2]);
                                if (success) {
                                    writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                                String contents = success ? "success" : "failure";
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (isFirstWord(command, "delete_directory_entry")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.deleteDirectoryEntry(Integer.parseInt(temp[1]), temp[2], temp[3], temp[4]);
                                if (success) {
                                    writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                            } else if (isFirstWord(command, "delete_file_from_upload_folder")) {
                                String[] temp = command.split("\t");
                                deleteLocalFile(temp[1]);
                            } else if (isFirstWord(command, "remove_directory")) {
//                                System.out.println("delete directory command received at port " + String.valueOf(myPort));
                                String[] temp = command.split("\t");
                                DeleteDirectory(temp[1]);
                            }
                        }
                    } catch (Exception ex) {
//                        ex.printStackTrace();
                        alreadyListening = false;
                        listener.close();
                    }
                } catch (BindException ignored) {
                    alreadyListening = false;
                    System.out.println("Another process is already listening on port " + String.valueOf(port));
                } catch (Exception ignored) {
                    alreadyListening = false;
                }
            }
        }).start();
    }

    public static boolean isFirstWord(String command, String query) {
        return command.indexOf(query) == 0;
    }

    private static void DeleteDirectory(final String directoryPath) {
        int key = getSHA1Hash(directoryPath);
        NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
        Document myDocument = myTable.entries.get(key).entry.get(directoryPath);
        myTable.deleteDocument(key, directoryPath);
        writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
        for (DirectoryEntry item : myDocument.directoryContents) {
            if (item.isFile) {
                String command;
                String pathname = directoryPath + item.name;
                int pathHash = getSHA1Hash(pathname);
                String filePort = getSuccessorPort(String.valueOf(pathHash));

                String actualFilePort = getFilePort(pathHash, pathname);
                command = "delete_file_from_upload_folder\t" + item.name + "\n";
                sendCommand(actualFilePort, command);

                if (!filePort.equals(String.valueOf(myPort))) {
                    command = "remove_document\t" + String.valueOf(pathHash) + "\t" + pathname + "\n";
                    sendCommandWithReturnValue(filePort, command);
                } else {
                    NodeResponsibilityTable myTable1 = getResponsibilityTable(String.valueOf(myPort));
                    boolean success = myTable1.deleteDocument(pathHash, pathname);
                    if (success) {
                        writeToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable1));
                    }
                }

                String directoryPath1 = getDirectoryFromFilePath(pathname);
                int directoryPathHash1 = getSHA1Hash(directoryPath1);
                String directoryPathPort1 = getSuccessorPort(String.valueOf(directoryPathHash1));
                command = "delete_directory_entry\t" + String.valueOf(directoryPathHash1) + "\t"
                        + directoryPath + "\t" + item.name + "\t" + "f" + "\n";
                sendCommand(directoryPathPort1, command);
            } else {
//                String pathname = directoryPath + item.name+ "/";
                String directoryPath1 = directoryPath + item.name + "/";
                int directoryPathHash1 = getSHA1Hash(directoryPath1);
                String directoryPathPort1 = getSuccessorPort(String.valueOf(directoryPathHash1));
                if (!directoryPathPort1.equals(String.valueOf(myPort))) {
                    String command = "remove_directory\t" + directoryPath1 + "\n";
                    sendCommand(directoryPathPort1, command);
                } else {
                    DeleteDirectory(directoryPath1);
                }

//                String directoryPath2 = getDirectoryFromFilePath(pathname);
//                int directoryPathHash2 = getSHA1Hash(directoryPath2);
//                String directoryPathPort2 = getSuccessorPort(String.valueOf(directoryPathHash2));
//                command = "delete_directory_entry\t" + String.valueOf(directoryPathHash2) + "\t"
//                        + directoryPath + "\t" + item.name + "\t" + "d" + "\n";
//                sendCommand(directoryPathPort2, command);
            }
        }
    }

    private static void deleteLocalFile(String filename) {
        File f = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + filename);
        if (f.exists() && !f.isDirectory()) {
            f.delete();
        }
    }

    private static void sendCommand(String port, String command) {
        try {
            Socket socket = new Socket(IP, Integer.valueOf(port));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeBytes(command);
            dos.flush();
            socket.close();
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
    }

    private static String sendCommandWithReturnValue(String port, String command) {
        try {
            Socket socket = new Socket(IP, Integer.valueOf(port));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeBytes(command);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String response = br.readLine();
            socket.close();
            return response;
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return "failure";
    }

    private static String getDirectoryFromFilePath(String filePath) {
        return filePath.substring(0, filePath.lastIndexOf("/") + 1);
    }

    private static String getBaseFile(String filepath) {
        return filepath.substring(filepath.lastIndexOf('/') + 1);
    }

    private static void UpdateFingerTableAndForwardCommand(String command, int currentPort, int currentHash, String nodeHash, String nodePort, String succHash, String succPort) {
        try {
            // If not hit back on current node update your current finger table
            // and forward the query
            if (Integer.parseInt(nodeHash) != currentHash) {
                updateFingerTable(String.valueOf(currentPort), nodeHash, nodePort, succHash);
                String mySuccessor = getOwnSuccessor(String.valueOf(currentPort));
                Socket socket1 = new Socket(IP, Integer.valueOf(mySuccessor));
                DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
                dos1.writeBytes(command);
                dos1.flush();
                dos1.close();
                socket1.close();
            }
            // hit back on current node
            else {

                // first find min responsibility of this node
                int min = 0;
                try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + String.valueOf(currentPort) + "_files"))) {
                    String line;
                    if ((line = reader.readLine()) != null) {
                        NodeResponsibilityTable myTable = new Gson().fromJson(line, NodeResponsibilityTable.class);
                        min = myTable.min;
                    }
                }
                ArrayList<String> contents = new ArrayList<>();
                for (int i = 1; i <= logN; i++) {
                    int temp1 = (currentHash + (int) Math.pow(2, i - 1)) % N;

                    // if the value of temp1 is between min and currentHash then successor of temp1
                    // is definitely currentHash
                    if (isBetween(min, currentHash, temp1)) {
                        contents.add(String.valueOf(temp1) + "\t" + currentHash + "\t" + String.valueOf(currentPort));
                    } else {
                        Socket socket1 = new Socket(IP, Integer.parseInt(succPort));
                        DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
                        String toWrite = "successor\t" + String.valueOf(temp1) + "\n";
                        dos1.writeBytes(toWrite);
                        dos1.flush();
                        InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
                        BufferedReader br1 = new BufferedReader(isr1);
                        String successorNodePort = br1.readLine();
                        String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
                        socket1.close();
                        contents.add(String.valueOf(temp1) + "\t" + successorNodeHash + "\t" + successorNodePort);
                    }
                }
                writeToFile(String.valueOf(currentPort), contents);
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
    }

    // update file responsibility table when a new node joins
    // filename: filename of the file to be updated
    // nodeHash1: nodeHash of the new node being joined
    // returns the responsibility of the newly formed node
    private static String updateFileResponsibility(String filename, String nodeHash1) {
        try {
            Integer nodeHash = Integer.parseInt(nodeHash1);
            Integer myHash = getHash(Integer.parseInt(filename));
            NodeResponsibilityTable myTable = new NodeResponsibilityTable();
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename + "_files"))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    myTable = new Gson().fromJson(line, NodeResponsibilityTable.class);
                }
            }

            NodeResponsibilityTable ownTable = new NodeResponsibilityTable();
            NodeResponsibilityTable toBeSentTable = new NodeResponsibilityTable();

            for (Integer key : myTable.entries.keySet()) {
                if (isBetween((nodeHash + 1) % N, myHash, key)) {
                    ownTable.entries.put(key, myTable.entries.get(key));
                } else {
                    toBeSentTable.entries.put(key, myTable.entries.get(key));
                }
            }
            toBeSentTable.min = myTable.min;
            ownTable.min = (nodeHash + 1) % N;
            writeToFileAsJson(filename + "_files", new Gson().toJson(ownTable));
            return new Gson().toJson(toBeSentTable);

        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return "";
    }


    // returns a nodes own successor. Node denoted by filename
    private static String getOwnSuccessor(String filename) {
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    String[] temp = line.split("\t");
                    return temp[2];
                }
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return "";
    }


    // returns successor port from a node denoted by filename and query node denoted by queryNodeId
    private static String getSuccessorPort(String queryNodeId) {
        String filename = String.valueOf(myPort);
        int min;
        int nodeId = getHash(Integer.parseInt(filename));
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename + "_files"))) {
            String line;
            if ((line = reader.readLine()) != null) {
                NodeResponsibilityTable myTable = new Gson().fromJson(line, NodeResponsibilityTable.class);
                min = myTable.min;
                if (isBetween(min, nodeId, Integer.parseInt(queryNodeId))) {
                    return filename;
                } else {
                    String successorPort = getNextPortFromFingerTable(filename, queryNodeId);
                    return getPortFromNextNode(queryNodeId, successorPort);
                }
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return "";
    }

    private static String getPortFromNextNode(String queryNodeId, String successorPort) {
        try {
            Socket socket = new Socket(IP, Integer.parseInt(successorPort));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "successor\t" + queryNodeId + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String successorNode = br.readLine();
            socket.close();
            return successorNode;
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return "";
    }


    // where to forward a query next
    private static String getNextPortFromFingerTable(String filename, String queryNodeId) {
        int key = Integer.parseInt(queryNodeId);
        String previous = "";
        String succ_1 = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
            String line;
            if ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                succ_1 = temp[2];
                previous = temp[2];
            }
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                if (Integer.parseInt(temp[0]) == key) {
                    return temp[2];
                } else if (Integer.parseInt(temp[0]) > key) {
                    return previous;
                }
                previous = temp[2];
            }
        } catch (Exception ignored) {

        }
        return succ_1;
    }


    // inclusive of min and max. in clockwise direction
    public static boolean isBetween(int min, int max, int query) {
        if (max == min) {
            return query == min;
        }
        if (max > min) {
            return query >= min && query <= max;
        } else {
            return query >= min || query <= max;
        }
    }

    static int getHash(int node) {
        return node % N;
    }

    private static void updateFingerTable(String filename, String nodeHash1, String nodePort1, String succHash1) {
        try {
            int nodeHash = Integer.parseInt(nodeHash1);
            int succHash = Integer.parseInt(succHash1);
            ArrayList<String> contents = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] temp = line.split("\t");
                    if (Integer.parseInt(temp[1]) == succHash) {
                        if (!isBetween((nodeHash + 1) % N, succHash, Integer.parseInt(temp[0]))) {
                            contents.add(temp[0] + "\t" + nodeHash1 + "\t" + nodePort1);
                        } else {
                            contents.add(line);
                        }
                    } else {
                        contents.add(line);
                    }
                }
                writeToFile(filename, contents);
            } catch (Exception ex) {
//                ex.printStackTrace();
            }

        } catch (Exception ex) {

        }
    }

    private static void writeToFileAsJson(String filename, String contents) {
        try {
            Path out = Paths.get(FINGER_TABLE_DIRECTORY + filename);
            Files.write(out, new ArrayList<>(Collections.singletonList(contents)), Charset.defaultCharset());
        } catch (Exception ignored) {
        }
    }

    private static void writeToFile(String filename, ArrayList<String> contents) {
        try {
            if (contents.get(0).equals("")) {
                contents.remove(0);
            }
            Path out = Paths.get(FINGER_TABLE_DIRECTORY + filename);
            Files.write(out, contents, Charset.defaultCharset());
        } catch (Exception ignored) {
        }
    }

    public static int getSHA1Hash(String id) {
        MessageDigest md;
        byte[] bytes = id.getBytes();
        try {
            md = MessageDigest.getInstance("SHA-1");
            byte[] temp = md.digest(bytes);
            int hash = temp[15];
            return hash % N < 0 ? (hash % N) + N : hash % N;
        } catch (NoSuchAlgorithmException e) {
            return 0;
        }
    }
}