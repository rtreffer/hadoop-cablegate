package com.github.cablegate;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class DistributedCacheDirectory extends Directory {

    protected HashMap<String, File> nameToFileMap =
        new HashMap<String, File>();
    protected HashMap<String, Directory> nameToDirectory =
        new HashMap<String, Directory>();

    public DistributedCacheDirectory(Path paths[]) throws IOException {
        for (Path path: paths) {
            File file = new File(path.toString());
            Directory dir = FSDirectory.open(file.getParentFile());
            nameToFileMap.put(path.getName(), file);
            nameToDirectory.put(path.getName(), dir);
        }
    }

    @Override
    public void close() throws IOException {
        for (Directory dir: nameToDirectory.values()) {
            dir.close();
        }
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return nameToDirectory.get(name).createOutput(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        nameToDirectory.get(name).deleteFile(name);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return nameToDirectory.containsKey(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return nameToFileMap.get(name).length();
    }

    @Override
    public long fileModified(String name) throws IOException {
        return nameToFileMap.get(name).lastModified();
    }

    @Override
    public String[] listAll() throws IOException {
        String names[] = new String[nameToDirectory.size()];
        return nameToDirectory.keySet().toArray(names);
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        return nameToDirectory.get(name).openInput(name);
    }

    @Override
    public void touchFile(String name) throws IOException {
        nameToDirectory.get(name).touchFile(name);
    }

}
