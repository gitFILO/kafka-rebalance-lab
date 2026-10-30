package lab;

public final class LabApp {

    private LabApp() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "";
        switch (mode) {
            case "producer" -> new LabProducer().run();
            case "consumer" -> new LabConsumer().run();
            case "lag-monitor" -> new LagMonitor().run();
            default -> {
                System.err.println("usage: LabApp <producer|consumer|lag-monitor>");
                System.exit(2);
            }
        }
    }
}
