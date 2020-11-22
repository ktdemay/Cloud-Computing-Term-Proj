import javax.swing.*;

public class term_proj extends JFrame{
    private JPanel panel1;
    private JButton searchB;
    private JButton topB;
    private JRadioButton shakeB;
    private JRadioButton hugoB;
    private JRadioButton tolsB;

    public term_proj() {
        ButtonGroup buttons = new ButtonGroup();
        buttons.add(shakeB);
        buttons.add(hugoB);
        buttons.add(tolsB);
        hugoB.setSelected(true);
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("term_proj");
        frame.setContentPane(new term_proj().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
