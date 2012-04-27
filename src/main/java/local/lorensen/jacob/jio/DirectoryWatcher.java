/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package local.lorensen.jacob.jio;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;
import java.util.logging.Logger;
import local.lorensen.jacob.util.CollectionUtils;
import local.lorensen.jacob.util.CollectionUtils.MapperProc;

/**
 *
 * @author JABLO
 */
public class DirectoryWatcher extends TimerTask {
    private final static Logger log = Logger.getLogger(DirectoryWatcher.class.getName());
    private final File dir;
    private long maxSize;
    private final ProblemHandler p;
    private final FilenameFilter fnFilter;
    private static final ProblemHandler defaultProblemHandler = new ProblemHandler() {
//        @Override
        public void errorDeletingFile(File f) {
            log.warning("Could not delete file " + f);
        }
    };

    public interface ProblemHandler {
        public void errorDeletingFile(File f);
    }

    public DirectoryWatcher(final File dir, final long maxSize) {
        this(dir, maxSize, defaultProblemHandler, null);
    }

    public DirectoryWatcher(final File dir, final long maxSize, ProblemHandler p, FilenameFilter fnFilter) {
        this.dir = dir;
        this.maxSize = maxSize;
        this.p = p;
        this.fnFilter = fnFilter;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    private List<File> recurseDirList(File dir) {
        File[] fs = dir.listFiles(fnFilter);
        List<File> result = new LinkedList();
        for (File f : fs) {
            if (f.isDirectory()) {
                List<File> subDirFs = recurseDirList(f);
                result.addAll(subDirFs);
            } else
                result.add(f);
        }
        return result;
    }

    @Override
    public void run() {
        List<File> files = recurseDirList(dir);
        Collections.sort(files, new Comparator<File>() {
//            @Override
            public int compare(File o1, File o2) {
                return Long.signum(o1.lastModified() - o2.lastModified());
            }
        });
        final long totalSize = CollectionUtils.fold(new CollectionUtils.Folder<File, Long>() {
//            @Override
            public Long f(Long a1, File a2) {
                return a1 + a2.length();
            }
        }, files, 0L);
        CollectionUtils.map(new MapperProc<File>() {
            private long totSiz = totalSize;
            public void f(File a) {
                if (totSiz <= maxSize)
                    return;
                totSiz -= a.length();
                if (!a.delete())
                    p.errorDeletingFile(a);
            }
        }, files);
    }
}
