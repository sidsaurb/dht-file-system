package com.cs425;

/**
 * Created by siddhant on 3/11/15.
 */
public class DirectoryEntry {
    public String name;
    public boolean isFile;

    public DirectoryEntry(String name, boolean isFile) {
        this.name = name;
        this.isFile = isFile;
    }
}
