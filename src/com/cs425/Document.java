package com.cs425;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by siddhant on 3/11/15.
 */
public class Document {
    //    public String pathName;
    public boolean isFile;
    public String fileLink;     // fileLink only valid if isFile = true
    public HashSet<DirectoryEntry> directoryContents; // only valid if isFile = false

    public Document(boolean isFile, String fileLink, HashSet<DirectoryEntry> directoryContents) {
//        this.pathName = pathName;
        this.isFile = isFile;
        this.fileLink = fileLink;
        this.directoryContents = directoryContents;
    }
}