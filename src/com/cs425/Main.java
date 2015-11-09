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


//    static int N = 65536;
//    static int logN = 16;

    static int N = 64;
    static int logN = 6;

    public static boolean alreadyListening = false;
    public static int myPort = 0;
    public static ServerSocket listener;


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
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
                        System.out.print("> Enter 5 digit port: ");
                        int port = scanner.nextInt();
                        int nodeHash = getHash(port);
                        alreadyListening = true;
                        ListenToSocket(nodeHash, port);
                        myPort = port;
                        if (port == MASTER_PORT) {
                            InitializeMasterPort(nodeHash);
                        } else {
                            InitializeOtherPorts(port, nodeHash);
                        }
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
                // just listen without joining
                case "l":
                    if (!alreadyListening) {
                        System.out.print("> Enter 5 digit port: ");
                        int port1 = scanner.nextInt();
                        int nodeHash1 = getHash(port1);
                        alreadyListening = true;
                        ListenToSocket(nodeHash1, port1);
                        myPort = port1;
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
//                // find a file location
//                case "f":
//                    if (alreadyListening) {
//                        System.out.print("> Enter file name: ");
//                        String filename = scanner.next();
//                        int fileHash = getSHA1Hash(filename);
//                        String filePort = getFilePort(fileHash);
//                        if (filePort.equals("null")) {
//                            System.out.println("The file does not exists");
//                        } else {
//                            System.out.println("The file exists at port: " + filePort);
//                        }
//                    } else {
//                        System.out.println("The program is not listening to any port");
//                    }
//                    break;
                // upload a file
                case "u":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename1 = scanner.next();
                        File f = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + filename1);
                        if (f.exists() && !f.isDirectory()) {
                            int pathHash1 = getSHA1Hash(CURRENT_PATH + filename1);
                            uploadFile(CURRENT_PATH + filename1, pathHash1);
                        } else {
                            System.out.println("File doesn't exist");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // download a file
                case "d":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename2 = scanner.next();
                        int fileHash2 = getSHA1Hash(CURRENT_PATH + filename2);
                        String filePort2 = getFilePort(fileHash2, CURRENT_PATH + filename2);
                        if (filePort2.equals("null")) {
                            System.out.println("The file does not exists");
                        } else {
                            DownloadFile(filePort2, filename2);
                            System.out.println("Downloaded from port: " + filePort2);
//                            System.out.println("The file exists at port: " + filePort2);
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // find route
//                case "r":
//                    if (alreadyListening) {
//                        System.out.print("> Enter file name: ");
//                        String filename2 = scanner.next();
//                        int fileHash2 = getSHA1Hash(filename2);
//                        String route = getFileRoute(fileHash2);
//                        System.out.println(route);
//                    } else {
//                        System.out.println("The program is not listening to any port");
//                    }
//                    break;
                case "pwd":
                    if (alreadyListening) {
                        System.out.println(CURRENT_PATH);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "ls":
                    if (alreadyListening) {
                        String contents = GetDirectoryContents();
                        System.out.println(contents);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "cd":
                    if (alreadyListening) {
//                        System.out.print("> Enter directory name: ");
                        String directoryName = scanner.next();
                        String[] arguments = directoryName.split("/");
                        for (String item1 : arguments) {
                            if (!item1.equals("..")) {
                                String contents = GetDirectoryContents();
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
                                    System.out.println("No such directory exists");
                                    break;
                                }
                            } else {
                                if (!CURRENT_PATH.equals("/filesystem/")) {
                                    CURRENT_PATH = getDirectoryFromFilePath(CURRENT_PATH.substring(0, CURRENT_PATH.length() - 1));
                                } else {
                                    System.out.println("No such directory exists");
                                    break;
                                }
                            }
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "mkdir":
                    if (alreadyListening) {
//                        System.out.print("> Enter directory name: ");
                        String directoryName = scanner.next();
                        if (createDirectory(CURRENT_PATH + directoryName + "/")) {
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
                        ProcessRmCommand(fileName);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                case "rmdir":
                    if (alreadyListening) {
                        String directoryName = scanner.next();
                        ProcessRmDirCommand(directoryName);
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

    private static void ProcessRmDirCommand(String directoryName) {
        String directoryPath = CURRENT_PATH + directoryName + "/";
        int directoryPathHash = getSHA1Hash(directoryPath);
        String directoryPathPort = getSuccessorPort(String.valueOf(directoryPathHash));
        String command = "remove_directory\t" + directoryPath + "\n";
        SendCommand(directoryPathPort, command);

        int currentDirectoryHash = getSHA1Hash(CURRENT_PATH);
        String currentDirectoryPort = getSuccessorPort(String.valueOf(currentDirectoryHash));
        command = "delete_directory_entry\t" + String.valueOf(currentDirectoryHash) + "\t"
                + CURRENT_PATH + "\t" + directoryName + "\t" + "d" + "\n";
        SendCommand(currentDirectoryPort, command);
        System.out.println("Directory deleted successfully");
    }

    private static void ProcessRmCommand(String fileName) {
        String command;
        String pathname = CURRENT_PATH + fileName;
        int pathHash = getSHA1Hash(pathname);
        String filePort = getSuccessorPort(String.valueOf(pathHash));

        String actualFilePort = getFilePort(pathHash, pathname);
        command = "delete_file_from_upload_folder\t" + fileName + "\n";
        SendCommand(actualFilePort, command);

        command = "remove_document\t" + String.valueOf(pathHash) + "\t" + pathname + "\n";
        String response = SendCommandWithReturnValue(filePort, command);

        if (response.equals("success")) {
            String directoryPath = getDirectoryFromFilePath(pathname);
            int directoryPathHash = getSHA1Hash(directoryPath);
            String directoryPathPort = getSuccessorPort(String.valueOf(directoryPathHash));
            command = "delete_directory_entry\t" + String.valueOf(directoryPathHash) + "\t"
                    + directoryPath + "\t" + fileName + "\t" + "f" + "\n";
            SendCommand(directoryPathPort, command);
            System.out.println("File deleted successfully");
        } else {
            System.out.println("File not found");
        }
    }

    private static boolean createDirectory(String directoryPath) {
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
                    WriteToFileAsJson(portOfDirectory + "_files", new Gson().toJson(myTable));
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
                    SendCommand(parentDirectoryPort, command1);
                } else {
                    NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                    myTable.updateDirectoryEntry(parentDirectoryHash, parentDirectoryPath, directoryName, "d");
                    WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                }
            }
            return success;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    private static String GetDirectoryContents() {
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
            ex.printStackTrace();
        }
        return "";
    }

    // takes port which has the file and downloads the file from it
    private static void DownloadFile(String filePort, String filename) {
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
            ex.printStackTrace();
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
                myTable.createFile(pathHash, pathName, String.valueOf(myPort));
                WriteToFileAsJson(portOfFile + "_files", new Gson().toJson(myTable));
                System.out.println("File uploaded");

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
                    System.out.println("File uploaded");
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
                SendCommand(directoryPort, command1);
            } else {
                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                myTable.updateDirectoryEntry(directoryHash, directoryPath, filename, "f");
                WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // update file responsibility table of the node, corresponding to the key fileHash to point
    // to containingPort
    private static void changeFilePort(String portOfFile, int fileHash, String containingPort) {
        ArrayList<String> contents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + portOfFile + "_files"))) {
            String line;
            line = reader.readLine();
            contents.add(line);
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                Integer fileKey = Integer.parseInt(temp[0]);
                if (fileHash == fileKey) {
                    contents.add(temp[0] + "\t" + containingPort);
                } else {
                    contents.add(line);
                }
            }
            WriteToFile(portOfFile + "_files", contents);
        } catch (Exception e) {
            e.printStackTrace();
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
//
//    private static String getFileRoute(int fileHash) {
//        try {
////            String portOfFile = getSuccessorPort(String.valueOf(getHash(myPort)), String.valueOf(fileHash));
//            String portOfFile = getSuccessorPort(String.valueOf((myPort)), String.valueOf(fileHash));
//            if (Integer.parseInt(portOfFile) == myPort) {
//                String fileDestinationPort = getFileDestinationPort(portOfFile, fileHash);
//                if (fileDestinationPort.equals("null")) {
//                    return String.valueOf(myPort) + " -> " + "null";
//                } else {
//                    return String.valueOf(myPort) + " -> " + fileDestinationPort;
//                }
//            } else {
//                Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
//                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
//                String toWrite = "route\t" + String.valueOf(fileHash) + "\n";
//                dos.writeBytes(toWrite);
//                dos.flush();
//                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
//                BufferedReader br = new BufferedReader(isr);
//                String route = br.readLine();
//                socket.close();
//                return String.valueOf(myPort) + " -> " + route;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return "null";
//    }

    private static NodeResponsibilityTable getResponsibilityTable(String portOfFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + portOfFile + "_files"))) {
            String line;
            if ((line = reader.readLine()) != null) {
                return new Gson().fromJson(line, NodeResponsibilityTable.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new NodeResponsibilityTable();
    }

    private static void InitializeOtherPorts(int port, int nodeHash) {
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
                WriteToFileAsJson(String.valueOf(port) + "_files", receivedData);

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

                CreateLocalDirectories();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void InitializeMasterPort(int nodeHash) {
        ArrayList<String> contents = new ArrayList<>();
        for (int i = 1; i <= logN; i++) {
            int temp = (nodeHash + (int) Math.pow(2, i - 1)) % N;
            contents.add(String.valueOf(temp) + "\t" + nodeHash + "\t" + String.valueOf(MASTER_PORT));
        }
        WriteToFile(String.valueOf(MASTER_PORT), contents);
//        ArrayList<String> responsibility = new ArrayList<>();

        NodeResponsibilityTable myTable = new NodeResponsibilityTable();
        for (int i = 0; i < N; i++) {
            myTable.entries.put(i, new NodeResponsibilityTableEntry());
//            responsibility.add(String.valueOf(i) + "\t[]");
        }
        myTable.min = (nodeHash + 1) % N;
//        responsibility.add(0, "min\t" + String.valueOf((nodeHash + 1) % N));

        int initialDirectoryHash = getSHA1Hash(CURRENT_PATH);
        if (!myTable.createDirectory(initialDirectoryHash, CURRENT_PATH)) {
            System.out.println("Error creating initial directory");
        }

        WriteToFileAsJson(String.valueOf(MASTER_PORT) + "_files", new Gson().toJson(myTable));
        CreateLocalDirectories();

    }

    private static void CreateLocalDirectories() {
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

    private static void ListenToSocket(final int hash, final int port) {
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
                            if (command.contains("successor")) {
                                String targetNode = command.split("\t")[1];
                                String a = getSuccessorPort(targetNode);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(a + "\n");
                                dos.flush();
                            } else if (command.contains("UpdateFingerTables")) {
                                String[] temp = command.split("\t");
                                String nodeHash = temp[1];
                                String nodePort = temp[2];
                                String succHash = temp[3];
                                String succPort = temp[4];
                                UpdateFingerTableAndForwardCommand(command, port, hash, nodeHash, nodePort, succHash, succPort);
                            } else if (command.contains("UpdateFileList")) {
                                String[] temp = command.split("\t");
                                String dataToSend = updateFileResponsibility(String.valueOf(port), temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (command.contains("filename")) {
                                // name is misleading. actually query for port number of a file.
                                String[] temp = command.split("\t");
                                String dataToSend = getFileDestinationPort(String.valueOf(port), Integer.parseInt(temp[1]), temp[2]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (command.contains("file_upload")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(port));
                                myTable.createFile(Integer.valueOf(temp[1]), temp[3], temp[2]);
                                WriteToFileAsJson(String.valueOf(port) + "_files", new Gson().toJson(myTable));
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes("success" + "\n");
                                dos.flush();
                            } else if (command.contains("downloadfile")) {
                                String[] temp = command.split("\t");
                                InputStream in = new FileInputStream(FILE_DIRECTORY + String.valueOf(port) + "/upload/" + temp[1]);
                                copy(in, os);
                                os.close();
                                in.close();
                            }
//                            else if (command.contains("route")) {
//                                String[] temp = command.split("\t");
//                                String a = getFileRoute(Integer.parseInt(temp[1]));
//                                DataOutputStream dos = new DataOutputStream(os);
//                                dos.writeBytes(a + "\n");
//                                dos.flush();
//                            }
                            else if (command.contains("update_directory_contents")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                myTable.updateDirectoryEntry(getSHA1Hash(temp[1]), temp[1], temp[2], temp[3]);
                                WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                            } else if (command.contains("get_directory_contents")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                int directoryHash = getSHA1Hash(temp[1]);
                                String contents = myTable.getContentsOfDirectory(directoryHash, temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (command.contains("mkdir")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.createDirectory(Integer.parseInt(temp[1]), temp[2]);
                                if (success) {
                                    WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                                String contents = success ? "success" : "failure";
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (command.contains("remove_document")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.deleteDocument(Integer.parseInt(temp[1]), temp[2]);
                                if (success) {
                                    WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                                String contents = success ? "success" : "failure";
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(contents + "\n");
                                dos.flush();
                                dos.close();
                            } else if (command.contains("delete_directory_entry")) {
                                String[] temp = command.split("\t");
                                NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
                                boolean success = myTable.deleteDirectoryEntry(Integer.parseInt(temp[1]), temp[2], temp[3], temp[4]);
                                if (success) {
                                    WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
                                }
                            } else if (command.contains("delete_file_from_upload_folder")) {
                                String[] temp = command.split("\t");
                                DeleteLocalFile(temp[1]);
                            } else if (command.contains("remove_directory")) {
//                                System.out.println("delete directory command received at port " + String.valueOf(myPort));
                                String[] temp = command.split("\t");
                                DeleteDirectory(temp[1]);
                            }
                        }
                    } catch (Exception ex) {
//                        ex.printStackTrace();
                        ex.printStackTrace();
                        alreadyListening = false;
                        listener.close();
                    }
                } catch (BindException ignored) {
                    alreadyListening = false;
                    System.out.println("Another process is already listening on port " + String.valueOf(port));
                } catch (Exception ignored) {
//                    ignored.printStackTrace();
                    alreadyListening = false;
                }
            }
        }).start();
    }

    private static void DeleteDirectory(final String directoryPath) {
        int key = getSHA1Hash(directoryPath);
        NodeResponsibilityTable myTable = getResponsibilityTable(String.valueOf(myPort));
        Document myDocument = myTable.entries.get(key).entry.get(directoryPath);
        myTable.deleteDocument(key, directoryPath);
        WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable));
        for (DirectoryEntry item : myDocument.directoryContents) {
            if (item.isFile) {
                String command;
                String pathname = directoryPath + item.name;
                int pathHash = getSHA1Hash(pathname);
                String filePort = getSuccessorPort(String.valueOf(pathHash));

                String actualFilePort = getFilePort(pathHash, pathname);
                command = "delete_file_from_upload_folder\t" + item.name + "\n";
                SendCommand(actualFilePort, command);

                if (!filePort.equals(String.valueOf(myPort))) {
                    command = "remove_document\t" + String.valueOf(pathHash) + "\t" + pathname + "\n";
                    SendCommandWithReturnValue(filePort, command);
                } else {
                    NodeResponsibilityTable myTable1 = getResponsibilityTable(String.valueOf(myPort));
                    boolean success = myTable1.deleteDocument(pathHash, pathname);
                    if (success) {
                        WriteToFileAsJson(String.valueOf(myPort) + "_files", new Gson().toJson(myTable1));
                    }
                }

//                String directoryPath1 = getDirectoryFromFilePath(pathname);
//                int directoryPathHash1 = getSHA1Hash(directoryPath1);
//                String directoryPathPort1 = getSuccessorPort(String.valueOf(directoryPathHash1));
//                command = "delete_directory_entry\t" + String.valueOf(directoryPathHash1) + "\t"
//                        + directoryPath + "\t" + item.name + "\t" + "f" + "\n";
//                SendCommand(directoryPathPort1, command);
            } else {
//                String pathname = directoryPath + item.name+ "/";
                String directoryPath1 = directoryPath + item.name + "/";
                int directoryPathHash1 = getSHA1Hash(directoryPath1);
                String directoryPathPort1 = getSuccessorPort(String.valueOf(directoryPathHash1));
                if (!directoryPathPort1.equals(String.valueOf(myPort))) {
                    String command = "remove_directory\t" + directoryPath1 + "\n";
                    SendCommand(directoryPathPort1, command);
                } else {
                    DeleteDirectory(directoryPath1);
                }

//                String directoryPath2 = getDirectoryFromFilePath(pathname);
//                int directoryPathHash2 = getSHA1Hash(directoryPath2);
//                String directoryPathPort2 = getSuccessorPort(String.valueOf(directoryPathHash2));
//                command = "delete_directory_entry\t" + String.valueOf(directoryPathHash2) + "\t"
//                        + directoryPath + "\t" + item.name + "\t" + "d" + "\n";
//                SendCommand(directoryPathPort2, command);
            }
        }
    }

    private static void DeleteLocalFile(String filename) {
        File f = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + filename);
        if (f.exists() && !f.isDirectory()) {
            f.delete();
        }
    }

    private static void SendCommand(String port, String command) {
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

    private static String SendCommandWithReturnValue(String port, String command) {
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
            ex.printStackTrace();
        }
        return "failure";
    }

    private static String getDirectoryFromFilePath(String filePath) {
        return filePath.substring(0, filePath.lastIndexOf("/") + 1);
    }

    private static void UpdateFingerTableAndForwardCommand(String command, int currentPort, int currentHash, String nodeHash, String nodePort, String succHash, String succPort) {
        try {
            // If not hit back on current node update your current finger table
            // and forward the query
            if (Integer.parseInt(nodeHash) != currentHash) {
                UpdateFingerTable(String.valueOf(currentPort), nodeHash, nodePort, succHash);
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
                WriteToFile(String.valueOf(currentPort), contents);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            WriteToFileAsJson(filename + "_files", new Gson().toJson(ownTable));
            return new Gson().toJson(toBeSentTable);

//            ArrayList<String> ownList = new ArrayList<>();
//            ArrayList<String> toBeSentList = new ArrayList<>();
//            ownList.add("min\t" + String.valueOf((nodeHash + 1) % N));
//            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename + "_files"))) {
//                String line;
//                if ((line = reader.readLine()) != null) {
//                    toBeSentList.add(line);
//                }
//                while ((line = reader.readLine()) != null) {
//                    String[] temp = line.split("\t");
//                    Integer fileKey = Integer.parseInt(temp[0]);
//                    if (isBetween((nodeHash + 1) % N, myHash, fileKey)) {
//                        ownList.add(line);
//                    } else {
//                        toBeSentList.add(line);
//                    }
//                }
//            }
//            WriteToFile(filename + "_files", ownList);
//            String toBeSent = "";
//            for (String item : toBeSentList) {
//                toBeSent += ";" + item;
//            }
//            return toBeSent;
        } catch (Exception ex) {
            ex.printStackTrace();
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
            ex.printStackTrace();
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
            ex.printStackTrace();
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
            e.printStackTrace();
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

    private static void UpdateFingerTable(String filename, String nodeHash1, String nodePort1, String succHash1) {
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
                WriteToFile(filename, contents);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        } catch (Exception ex) {

        }
    }

    private static void WriteToFileAsJson(String filename, String contents) {
        try {
//            if (contents.get(0).equals("")) {
//                contents.remove(0);
//            }
            Path out = Paths.get(FINGER_TABLE_DIRECTORY + filename);
            Files.write(out, new ArrayList<>(Arrays.asList(contents)), Charset.defaultCharset());
        } catch (Exception ignored) {
        }
    }

    private static void WriteToFile(String filename, ArrayList<String> contents) {
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


