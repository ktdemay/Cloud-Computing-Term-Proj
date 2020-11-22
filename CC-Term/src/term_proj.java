import javax.swing.*;

public class term_proj {
    private JPanel panel1;
    private JButton searchB;
    private JButton topB;

    public static void main(String[] args) {
        JFrame frame = new JFrame("term_proj");
        frame.setContentPane(new term_proj().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
