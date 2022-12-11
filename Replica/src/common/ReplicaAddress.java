package common;

public class ReplicaAddress {
    private final String ip;
    private final int port;

    public ReplicaAddress(String address) {
        String[] content = address.split(":");
        this.ip = content[0];
        this.port = Integer.parseInt(content[1]);

    }
    public String getIp() {
        return ip;
    }
    public int getPort() {
        return port;
    }
    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
