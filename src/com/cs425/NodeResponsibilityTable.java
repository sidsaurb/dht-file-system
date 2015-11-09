package com.cs425;

import javax.print.Doc;
import java.util.*;

/**
 * Created by siddhant on 4/11/15.
 */
public class NodeResponsibilityTable {
    public HashMap<Integer, NodeResponsibilityTableEntry> entries;
    public int min; // minimum responsibility of this node

    public NodeResponsibilityTable() {
        this.entries = new HashMap<>();
    }

    public NodeResponsibilityTable(HashMap<Integer, NodeResponsibilityTableEntry> entries) {
        this.entries = entries;
    }

    // create/ overwrite a file but does not modify directory entry
    public void createFile(int key, String filepath, String port) {
        Document newDocument = new Document(true, port, new HashSet<DirectoryEntry>());
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        if (!temp.entry.containsKey(filepath)) {
            temp.entry.put(filepath, newDocument);
        } else {
            temp.entry.put(filepath, newDocument);
            // send delete command to delete old file
        }

    }

    public boolean createDirectory(int key, String directoryPath) {
        Document newDocument = new Document(false, "", new HashSet<DirectoryEntry>());
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        if (!temp.entry.containsKey(directoryPath)) {
            temp.entry.put(directoryPath, newDocument);
            return true;
        } else {
            return false;
        }
    }

    public void updateDirectoryEntry(int key, String directoryPath, String name, String type) {
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        Document doc = temp.entry.get(directoryPath);
        DirectoryEntry newEntry;
        if (type.equals("f")) {
            newEntry = new DirectoryEntry(name, true);
        } else {
            newEntry = new DirectoryEntry(name, false);
        }
        // check if equality is inferred correctly
        boolean exists = false;
        for (DirectoryEntry a : doc.directoryContents) {
            if (a.name.equals(newEntry.name) && a.isFile == newEntry.isFile) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            doc.directoryContents.add(newEntry);
        }
    }

    // only works for deleting file
    public boolean deleteDirectoryEntry(int key, String directoryPath, String name, String type) {
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        Document doc = temp.entry.get(directoryPath);
        try {
            Iterator<DirectoryEntry> it = doc.directoryContents.iterator();
            DirectoryEntry toBeDeleted = null;
            while (it.hasNext()) {
                DirectoryEntry temp1 = it.next();
                if (temp1.name.equals(name)) {
                    toBeDeleted = temp1;
                    break;
                }
            }
            if (toBeDeleted != null) {
                doc.directoryContents.remove(toBeDeleted);
                return true;
            } else {
                return false;
            }
        } catch (Exception ignored) {
            System.out.println(String.valueOf(key) + " " + directoryPath + " " + name);
            return false;
        }
    }

    public String getContentsOfDirectory(int key, String directoryPath) {
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        Document doc = temp.entry.get(directoryPath);
        String result = "";
        for (DirectoryEntry item : doc.directoryContents) {
            result += item.name + "\t" + (item.isFile ? "f" : "d") + "\n";
        }
        return result;
    }

    public boolean deleteDocument(int key, String pathname) {
        NodeResponsibilityTableEntry temp = this.entries.get(key);
        Document doc = temp.entry.remove(pathname);
        return doc != null;
    }


}