import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Dispatcher {

    public static void main(String[] args) {

        new Thread(new Client()).start();
    }

    private static class Client implements Runnable {

        private DatagramPacket packet;

        private DatagramSocket socket;
        private InetAddress address;

        private byte[] buf = new byte[60 * 1024];
        private int PORT = 50000;
        private String kek = "127.0.0.1";

        private Scanner scanner;

        int clientNumber;

        private Listener listener;

        @Override
        public void run() {
            scanner = new Scanner(System.in);

            try {
                socket = new DatagramSocket();
                address = InetAddress.getByName(kek);
            } catch (SocketException | UnknownHostException e) {
                e.printStackTrace();
            }

            try {
                packet = new DatagramPacket(new byte[] {'0'}, 1, address, PORT);
                socket.send(packet);
                socket.receive(packet);
                clientNumber = packet.getData()[0];
                System.out.println("Number = " + clientNumber);
            } catch (IOException e) {
                e.printStackTrace();
            }

            listener = new Listener(socket);
            new Thread(listener).start();

            MyInt countK = null;

            while(true) {

                String path = scanner.nextLine();

                try {
                    FileInputStream fileInputStream = new FileInputStream(path);
                    byte j = 0;
                    countK = new MyInt(f(fileInputStream.available()));
//                    System.out.println("File size = " + fileInputStream.available());
                    while(fileInputStream.available() > 0) {
                        if(fileInputStream.available() < buf.length) {
                            buf = new byte[fileInputStream.available()];
                        }
                        fileInputStream.read(buf, 0, buf.length);
                        buf = concat(new byte[]{(byte) clientNumber, j++}, buf);
//                        System.out.println("Sent: " + Arrays.toString(buf));
                        packet = new DatagramPacket(buf, buf.length, address, PORT);
                        socket.send(packet);
                        buf = new byte[60 * 1024];
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // Задержка
//                for(int i = 0; i < 10000000; ++i);

                System.out.println("coutnt" + countK.value);

                while(!countK.equals(listener.getBuffersCount()));

                try {
                    // TODO: string "end"
                    DatagramPacket datagramPacket = new DatagramPacket(new byte[] {(byte) clientNumber, -1}, 2, address, PORT);
                    socket.send(datagramPacket);
//                    System.out.println("Sent: " + Arrays.toString(datagramPacket.getData()));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                buf = new byte[60 * 1024];
            }
        }

        private int f(boolean d) {
            return d ? 1 : 0;
        }

        private int f(int size) {
            return size / (60 * 1024) + f((size % (60 * 1024)) == 0);
        }

        private class MyInt {
            public int getValue() {
                return value;
            }

            public void setValue(int value) {
                this.value = value;
            }

            private int value;

            public MyInt(int value) {
                this.value = value;
            }

            @Override
            public boolean equals(Object obj) {
                try {
                    return this.value == ((MyInt) obj).value;
                }
                catch (Exception e) {
                    return false;
                }
            }

            public boolean equals(int a) {
                return value == a;
            }
        }

        private byte[] concat(byte[] a, byte[] b) {
            byte[] t = new byte[a.length + b.length];
            System.arraycopy(a, 0, t, 0, a.length);
            System.arraycopy(b, 0, t, a.length, b.length);
            return t;
        }

        class Listener implements Runnable {

            private DatagramSocket socket;

            Listener(DatagramSocket socket) {
                this.socket = socket;
            }

            @Override
            public void run() {
                while(true) {
                    try {
                        receive();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            byte[][] buffers = new byte[256][];

            public int getBuffersCount() {
                return buffersCount;
            }

            int buffersCount = 0;

            private void receive() throws IOException {
                byte[] buf = new byte[60 * 1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
//                System.out.println("Receiving");
                socket.receive(packet);
//                System.out.println("Received. Size = " + packet.getLength());
//                System.out.println("Received: " + Arrays.toString(packet.getData()));
                byte[] lol = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), 0, lol, 0, packet.getLength());
//                System.out.println("length " + packet.getLength() + " " + buf.length);
                if(lol[0] == -1) {
                    writeFile();
                    buffersCount = 0;
                    buffers = new byte[256][];
                    return;
                }
                int k = lol[0];
                lol = deleteFirstByte(lol);
                System.out.println("k = " + k);
                buffers[k] = lol;
                ++buffersCount;
            }

            private byte[] deleteFirstByte(byte[] s) {
                byte[] ans = new byte[s.length - 1];
                System.arraycopy(s, 1, ans, 0, s.length - 1);
                return ans;
            }

            private synchronized void writeFile() throws IOException {
//                System.out.println("writing file");
                File file = new File("D:\\java\\keeek.txt");
                if(!file.exists()) {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println("buffersCount = " + buffersCount);

                FileOutputStream fileOutputStream = new FileOutputStream("D:\\java\\keeek.mp4");
                for(int i = 0; i < buffersCount; ++i) {
                    System.out.println(i + ": " + buffers[i]);
                    fileOutputStream.write(buffers[i]);
                }
            }
        }
    }
}