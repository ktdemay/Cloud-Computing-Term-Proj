import javax.swing.*;
import java.awt.*;

import com.spotify.dataproc.DataprocHadoopRunner;
import com.spotify.dataproc.Job;
import com.spotify.dataproc.submitter.DataprocLambdaRunner;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class term_proj extends JFrame{
    private JPanel panel1;
    private JButton searchB;
    private JButton topB;
    private JRadioButton shakeB;
    private JRadioButton hugoB;
    private JRadioButton tolsB;
    private JTextField searchTF;
    private JTextField topTF;
    private JTextArea resultsBox;
    private JButton dispTN;
    private JButton ciiB;

    public term_proj() throws IOException {
        ButtonGroup buttons = new ButtonGroup();
        buttons.add(shakeB);
        buttons.add(hugoB);
        buttons.add(tolsB);
        hugoB.setSelected(true);

        resultsBox.setEditable(false);
        resultsBox.setPreferredSize(new Dimension(450, 100));

        topB.addActionListener(new ActionListener() {
            String author = "";
            @Override
            public void actionPerformed(ActionEvent e) {
                if(hugoB.isSelected()) {
                    author = "Hugo";
                }
                else if(shakeB.isSelected()) {
                    author = "Shakespeare";
                }
                else if(tolsB.isSelected()) {
                    author = "Tolstoy";
                }

                String n = topTF.getText();

                String project = "cloudcomputinghomework3";
                String cluster = "cluster-382e";
                List<String> GCPargs = new ArrayList<>();
                GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/TopN/docs/" + author);
                GCPargs.add("gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/TopN/out/" + author);
                GCPargs.add(n);

                String[] jars = {"gs://dataproc-staging-us-central1-755152546030-wxtwz1dg/TopN/genericDriver.jar"};

                DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder(project, cluster).build();

                Job job = Job.builder().setMainClass("genericDriver").setArgs(GCPargs).setShippedJars(jars).createJob();

                try {
                    hadoopRunner.submit(job);
                } catch (Exception ex) {
                    System.out.println(ex);
                }
            }
        });

        dispTN.addActionListener(new ActionListener() {
            String author = "";
            String project = "cloudcomputinghomework3";
            @Override
            public void actionPerformed(ActionEvent e) {
                if(hugoB.isSelected()) {
                    author = "Hugo";
                }
                else if(shakeB.isSelected()) {
                    author = "Shakespeare";
                }
                else if(tolsB.isSelected()) {
                    author = "Tolstoy";
                }

                try {
                    Process proc = Runtime.getRuntime().exec("java -jar test.jar " + author);

                    File f = new File("out.txt");
                    Scanner scan = new Scanner(f);
                    while(scan.hasNextLine()) {
                        String curr = scan.nextLine();
                        System.out.println(curr);
                        String count = curr.substring(0, curr.indexOf("\t"));
                        String word = curr.substring(curr.indexOf("\t"));
                        resultsBox.append(word + ": " + count + "\n");
                    }
                } catch(Exception fe) {
                    System.out.printf(fe.toString());
                }
            }
        });
    }

    public static void main(String[] args) throws IOException {
        JFrame frame = new JFrame("term_proj");
        frame.setContentPane(new term_proj().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
