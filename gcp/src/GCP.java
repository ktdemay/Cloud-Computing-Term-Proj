import com.spotify.dataproc.DataprocHadoopRunner;
import com.spotify.dataproc.Job;
import com.spotify.dataproc.submitter.DataprocLambdaRunner;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCP
{
    public static void main(String[] args) throws IOException
    {
        String project = "cloudcomputinghomework3";
//        String cluster = "cluster-382e";
//        List<String> GCPargs = new ArrayList<>();
//        GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/MapReduce/wordcountres/wordcountres");
//        GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/MapReduce/javatest2");
//        GCPargs.add("5");
//
//        String[] jars = {"gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/TopN/genericDriver.jar"};
//
//        DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder(project, cluster).build();
//
//        Job job = Job.builder()
//                .setMainClass("genericDriver")
//    .setArgs(GCPargs)
//    .setShippedJars(jars)
//    .createJob();
//
//
//        hadoopRunner.submit(job);
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(project)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream("auth.json")))
                .build()
                .getService();
        Blob blob = storage.get("dataproc-staging-us-central1-755152546030-wxtwz1dg", "TopN/out/" + "Shakespeare" + "/part-r-00000");
        ReadChannel readChannel = blob.reader();
        FileOutputStream fileOutputStream = new FileOutputStream("out.txt");
        fileOutputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
        fileOutputStream.close();
    }
}
