//第二部分代码
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsApiDemo {
    private static FileSystem fs;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy‑MM‑dd HH:mm:ss");

    static {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("=====1.上传文件：存在时选择覆盖true /追加false =====");
        uploadFile("/home/hadoop/testfile.txt","/user/hadoop/api_test.txt",true);

        System.out.println("\n=====2.HDFS下载文件，本地重命名 =====");
        downloadFile("/user/hadoop/api_test.txt","/home/hadoop/download/api_test.txt");

        System.out.println("\n=====3.输出HDFS文件内容到终端 =====");
        catFile("/user/hadoop/api_test.txt");

        System.out.println("\n=====4.打印单个文件元信息(权限,大小,时间,路径) =====");
        printFileStatus("/user/hadoop/api_test.txt");

        System.out.println("\n=====5.递归遍历目录所有文件元信息 =====");
        listAllFileStatus("/user/hadoop");

        System.out.println("\n=====6.创建/删除HDFS文件，不存在目录自动建 =====");
        createOrDelFile("/user/hadoop/api_dir/test_create.txt",true);

        System.out.println("\n=====7.创建/删除目录；非空目录禁止删除 =====");
        createOrDelDir("/user/hadoop/api_mkdir/testsub",true);

        System.out.println("\n=====8.追加内容：true=头部，false=尾部 =====");
        appendContent("/user/hadoop/api_test.txt","【追加测试内容】",false);

        System.out.println("\n=====9.删除HDFS指定文件 =====");
        deleteHdfsFile("/user/hadoop/api_dir/test_create.txt");

        System.out.println("\n=====10.移动HDFS文件 =====");
        moveFile("/user/hadoop/api_test.txt","/user/hadoop/api_mkdir/testsub/api_test_moved.txt");
    }


    //1.上传文件，isOverwrite true覆盖，false追加
    public static void uploadFile(String localPath, String hdfsPath, boolean isOverwrite) throws IOException {
        Path local = new Path(localPath);
        Path hdfs = new Path(hdfsPath);
        if(fs.exists(hdfs)){
            if(isOverwrite){
                fs.copyFromLocalFile(false,true,local,hdfs);
                System.out.println("文件已存在，执行覆盖上传");
            }else{
                //追加
                FSDataOutputStream out = fs.append(hdfs);
                FileInputStream in = new FileInputStream(new File(localPath));
                IOUtils.copyBytes(in,out,4096,false);
                IOUtils.closeStream(in);
                IOUtils.closeStream(out);
                System.out.println("文件已存在，执行追加上传");
            }
        }else{
            fs.copyFromLocalFile(local,hdfs);
            System.out.println("文件不存在，直接上传");
        }
    }

    //2.下载HDFS文件，本地同名自动重命名
    public static void downloadFile(String hdfsPath,String localPath) throws IOException {
        Path hdfs = new Path(hdfsPath);
        File localFile = new File(localPath);
        File outFile = localFile;
        int count=1;
        while(outFile.exists()){
            String name = localFile.getName();
            int dot = name.lastIndexOf('.');
            if(dot>0){
                String prefix = name.substring(0,dot);
                String suffix = name.substring(dot);
                outFile = new File(localFile.getParent(),prefix+"_"+count+suffix);
            }else{
                outFile = new File(localFile.getParent(),name+"_"+count);
            }
            count++;
        }
        fs.copyToLocalFile(false,hdfs,new Path(outFile.getAbsolutePath()),true);
        System.out.println("下载完成，本地输出路径："+outFile.getAbsolutePath());
    }

    //3.输出HDFS文件内容到控制台
    public static void catFile(String hdfsPath) throws IOException {
        FSDataInputStream in = fs.open(new Path(hdfsPath));
        IOUtils.copyBytes(in,System.out,4096,false);
        IOUtils.closeStream(in);
    }

    //4.打印单个文件状态：权限，大小，修改时间，路径
    public static void printFileStatus(String hdfsPath) throws IOException {
        FileStatus stat = fs.getFileStatus(new Path(hdfsPath));
        System.out.println("路径："+stat.getPath());
        System.out.println("权限："+stat.getPermission());
        System.out.println("大小(byte)："+stat.getLen());
        System.out.println("修改时间："+sdf.format(new Date(stat.getModificationTime())));
    }

    //5.递归遍历目录，输出全部文件元信息
    public static void listAllFileStatus(String dir) throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(dir),true);
        while(it.hasNext()){
            LocatedFileStatus s = it.next();
            System.out.println("=======");
            System.out.println("路径："+s.getPath());
            System.out.println("权限："+s.getPermission());
            System.out.println("大小："+s.getLen());
            System.out.println("修改时间："+sdf.format(new Date(s.getModificationTime())));
        }
    }

    //6.创建/删除文件，父目录自动创建；op=true创建，false删除
    public static void createOrDelFile(String path,boolean op) throws IOException {
        Path p = new Path(path);
        if(op){
            FSDataOutputStream out = fs.create(p,true);
            out.close();
            System.out.println("文件创建成功："+path);
        }else{
            if(fs.exists(p)){
                fs.delete(p,false);
                System.out.println("文件删除成功");
            }
        }
    }

    //7.目录创建删除；op=true创建；false删除，仅空目录允许删除
    public static void createOrDelDir(String dirPath,boolean op) throws IOException {
        Path p = new Path(dirPath);
        if(op){
            fs.mkdirs(p);
            System.out.println("目录创建成功："+dirPath);
        }else{
            if(!fs.exists(p)) return;
            FileStatus[] files = fs.listStatus(p);
            if(files.length>0){
                System.out.println("目录非空，禁止删除");
            }else{
                fs.delete(p,false);
                System.out.println("空目录删除成功");
            }
        }
    }

    //8.向文件追加内容
    public static void appendContent(String filePath,String content,boolean isHead) throws IOException {
        Path p = new Path(filePath);
        if(!fs.exists(p)){
            System.err.println("文件不存在");
            return;
        }
        if(isHead){
            FSDataInputStream in = fs.open(p);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            IOUtils.copyBytes(in,buf,4096,false);
            IOUtils.closeStream(in);
            byte[] oldData = buf.toByteArray();
            FSDataOutputStream out = fs.create(p,true);
            out.write(content.getBytes());
            out.write(oldData);
            out.close();
            System.out.println("内容追加至文件头部");
        }else{
            FSDataOutputStream out = fs.append(p);
            out.write(content.getBytes());
            out.close();
            System.out.println("内容追加至文件尾部");
        }
    }

    //9.删除HDFS指定文件
    public static void deleteHdfsFile(String path) throws IOException {
        Path p = new Path(path);
        if(fs.exists(p)){
            fs.delete(p,false);
            System.out.println("已删除文件："+path);
        }else{
            System.out.println("文件不存在");
        }
    }

    //10.移动HDFS文件
    public static void moveFile(String src,String dst) throws IOException {
        Path source = new Path(src);
        Path dest = new Path(dst);
        boolean ok = fs.rename(source,dest);
        if(ok){
            System.out.println("移动成功 src:"+src+" → dst:"+dst);
        }else{
            System.err.println("移动失败");
        }
    }
}
