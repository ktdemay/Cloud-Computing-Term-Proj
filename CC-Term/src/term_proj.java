import javax.swing.*;

public class term_proj {
    private JPanel panel1;
    private JButton rstudioB;
    private JButton spyderB;
    private JButton ibmB;
    private JButton gitB;
    private JButton orangeB;
    private JButton vsB;
    private JButton hadoopB;
    private JButton sparkB;
    private JButton tableauB;
    private JButton ssB;
    private JButton tensorB;
    private JButton mdB;
    private JButton npB;
    private JButton jupyterB;

    public static void main(String[] args) {
        JFrame frame = new JFrame("term_proj");
        frame.setContentPane(new term_proj().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
