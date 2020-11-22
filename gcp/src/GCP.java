import com.spotify.dataproc.DataprocHadoopRunner;
import com.spotify.dataproc.Job;
import com.spotify.dataproc.submitter.DataprocLambdaRunner;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GCP
{
    public static void main(String[] args) throws IOException
    {
        String project = "cloudcomputinghomework3";
        String cluster = "cluster-382e";
        List<String> GCPargs = new ArrayList<>();
        GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/MapReduce/wordcountres/wordcountres");
        GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/MapReduce/javatest2");
        GCPargs.add("5");

        String[] jars = {"gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/TopN/genericDriver.jar"};

        DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder(project, cluster).build();

        Job job = Job.builder()
                .setMainClass("genericDriver")
    .setArgs(GCPargs)
    .setShippedJars(jars)
    .createJob();


        hadoopRunner.submit(job);
    }
}
