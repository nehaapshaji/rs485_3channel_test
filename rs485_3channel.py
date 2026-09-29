import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import Int32


class ThreeChannelContinuousTest(Node):

    def __init__(self):
        super().__init__("three_channel_continuous_test")

        # ============================================================
        # BEST EFFORT QoS
        # ============================================================
        best_effort_qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10
        )

        # ============================================================
        # PUBLISHERS
        # ROS -> ESP32
        # ============================================================
        self.ping1_pub = self.create_publisher(
            Int32, "/ping1", best_effort_qos
        )

        self.ping2_pub = self.create_publisher(
            Int32, "/ping2", best_effort_qos
        )

        self.ping3_pub = self.create_publisher(
            Int32, "/ping3", best_effort_qos
        )

        # ============================================================
        # SUBSCRIBERS
        # ESP32 -> ROS
        # ============================================================
        self.pong1_sub = self.create_subscription(
            Int32,
            "/pong1",
            self.pong1_callback,
            best_effort_qos
        )

        self.pong2_sub = self.create_subscription(
            Int32,
            "/pong2",
            self.pong2_callback,
            best_effort_qos
        )

        self.pong3_sub = self.create_subscription(
            Int32,
            "/pong3",
            self.pong3_callback,
            best_effort_qos
        )

        # ============================================================
        # NEXT VALUES TO SEND
        # ROS sends:
        # 0, 2, 4, 6, 8, ...
        # ESP32 should respond:
        # 1, 3, 5, 7, 9, ...
        # ============================================================
        self.ch1_next = 0
        self.ch2_next = 0
        self.ch3_next = 0

        # ============================================================
        # TX COUNTERS
        # ============================================================
        self.ch1_tx = 0
        self.ch2_tx = 0
        self.ch3_tx = 0

        # ============================================================
        # RX COUNTERS
        # ============================================================
        self.ch1_rx = 0
        self.ch2_rx = 0
        self.ch3_rx = 0

        # ============================================================
        # MISSED MESSAGE COUNTERS
        # ============================================================
        self.ch1_missed = 0
        self.ch2_missed = 0
        self.ch3_missed = 0

        # ============================================================
        # LAST RECEIVED VALUE
        # None means no message received yet.
        # ============================================================
        self.ch1_last_rx = None
        self.ch2_last_rx = None
        self.ch3_last_rx = None

        # ============================================================
        # STARTUP
        # ============================================================
        self.started = False

        self.start_timer = self.create_timer(
            1.0,
            self.start_test
        )

        # ============================================================
        # INDEPENDENT CHANNEL TIMERS
        # Each channel sends once every 100 ms.
        # ============================================================
        self.ch1_timer = self.create_timer(
            0.1,
            self.send_ch1
        )

        self.ch2_timer = self.create_timer(
            0.1,
            self.send_ch2
        )

        self.ch3_timer = self.create_timer(
            0.1,
            self.send_ch3
        )

        # Don't start sending until start_test()
        self.ch1_timer.cancel()
        self.ch2_timer.cancel()
        self.ch3_timer.cancel()

        # ============================================================
        # STARTUP MESSAGE
        # ============================================================
        self.get_logger().info("========================================")
        self.get_logger().info(
            "3-CHANNEL CONTINUOUS BEST EFFORT TEST"
        )
        self.get_logger().info("========================================")
        self.get_logger().info(
            "No deliberate packet drops"
        )
        self.get_logger().info(
            "Natural packet loss will be detected"
        )

    # ================================================================
    # START TEST
    # ================================================================
    def start_test(self):

        if self.started:
            return

        self.started = True
        self.start_timer.cancel()

        self.get_logger().info(
            "STARTING ALL 3 CHANNELS"
        )

        # Send first message immediately
        self.send_ch1()
        self.send_ch2()
        self.send_ch3()

        # Start independent timers
        self.ch1_timer.reset()
        self.ch2_timer.reset()
        self.ch3_timer.reset()

        self.get_logger().info("========================================")
        self.get_logger().info(
            "CH1, CH2, CH3 RUNNING INDEPENDENTLY"
        )
        self.get_logger().info(
            "BEST EFFORT QoS ACTIVE"
        )
        self.get_logger().info(
            "PACKET LOSS DETECTION ACTIVE"
        )
        self.get_logger().info("========================================")

    # ================================================================
    # CHANNEL 1 TX
    # ================================================================
    def send_ch1(self):

        if not self.started:
            return

        msg = Int32()
        msg.data = self.ch1_next

        self.ping1_pub.publish(msg)

        self.ch1_tx += 1

        self.get_logger().info(
            f"[CH1] TX {self.ch1_next}"
        )

        self.ch1_next += 2

    # ================================================================
    # CHANNEL 2 TX
    # ================================================================
    def send_ch2(self):

        if not self.started:
            return

        msg = Int32()
        msg.data = self.ch2_next

        self.ping2_pub.publish(msg)

        self.ch2_tx += 1

        self.get_logger().info(
            f"[CH2] TX {self.ch2_next}"
        )

        self.ch2_next += 2

    # ================================================================
    # CHANNEL 3 TX
    # ================================================================
    def send_ch3(self):

        if not self.started:
            return

        msg = Int32()
        msg.data = self.ch3_next

        self.ping3_pub.publish(msg)

        self.ch3_tx += 1

        self.get_logger().info(
            f"[CH3] TX {self.ch3_next}"
        )

        self.ch3_next += 2

    # ================================================================
    # CHANNEL 1 RX
    # ================================================================
    def pong1_callback(self, msg):

        received = msg.data

        self.ch1_rx += 1

        # First received message
        if self.ch1_last_rx is None:

            self.get_logger().info(
                f"[CH1] RX {received}"
            )

        else:

            expected = self.ch1_last_rx + 2

            if received != expected:

                if received > expected:

                    missed = (received - expected) // 2

                    self.ch1_missed += missed

                    self.get_logger().warn(
                        f"[CH1] ⚠ MISSED {missed} MESSAGE(S): "
                        f"expected {expected}, received {received}"
                    )

                else:

                    self.get_logger().warn(
                        f"[CH1] ⚠ UNEXPECTED: "
                        f"expected {expected}, received {received}"
                    )

            else:

                self.get_logger().info(
                    f"[CH1] RX {received}"
                )

        self.ch1_last_rx = received

    # ================================================================
    # CHANNEL 2 RX
    # ================================================================
    def pong2_callback(self, msg):

        received = msg.data

        self.ch2_rx += 1

        # First received message
        if self.ch2_last_rx is None:

            self.get_logger().info(
                f"[CH2] RX {received}"
            )

        else:

            expected = self.ch2_last_rx + 2

            if received != expected:

                if received > expected:

                    missed = (received - expected) // 2

                    self.ch2_missed += missed

                    self.get_logger().warn(
                        f"[CH2] ⚠ MISSED {missed} MESSAGE(S): "
                        f"expected {expected}, received {received}"
                    )

                else:

                    self.get_logger().warn(
                        f"[CH2] ⚠ UNEXPECTED: "
                        f"expected {expected}, received {received}"
                    )

            else:

                self.get_logger().info(
                    f"[CH2] RX {received}"
                )

        self.ch2_last_rx = received

    # ================================================================
    # CHANNEL 3 RX
    # ================================================================
    def pong3_callback(self, msg):

        received = msg.data

        self.ch3_rx += 1

        # First received message
        if self.ch3_last_rx is None:

            self.get_logger().info(
                f"[CH3] RX {received}"
            )

        else:

            expected = self.ch3_last_rx + 2

            if received != expected:

                if received > expected:

                    missed = (received - expected) // 2

                    self.ch3_missed += missed

                    self.get_logger().warn(
                        f"[CH3] ⚠ MISSED {missed} MESSAGE(S): "
                        f"expected {expected}, received {received}"
                    )

                else:

                    self.get_logger().warn(
                        f"[CH3] ⚠ UNEXPECTED: "
                        f"expected {expected}, received {received}"
                    )

            else:

                self.get_logger().info(
                    f"[CH3] RX {received}"
                )

        self.ch3_last_rx = received

    # ================================================================
    # FINAL STATISTICS
    # ================================================================
    def print_statistics(self):

        self.get_logger().info("")
        self.get_logger().info("========================================")
        self.get_logger().info("FINAL STATISTICS")
        self.get_logger().info("========================================")

        self.get_logger().info(
            f"CH1: TX={self.ch1_tx}, "
            f"RX={self.ch1_rx}, "
            f"MISSED={self.ch1_missed}, "
            f"LAST_RX={self.ch1_last_rx}"
        )

        self.get_logger().info(
            f"CH2: TX={self.ch2_tx}, "
            f"RX={self.ch2_rx}, "
            f"MISSED={self.ch2_missed}, "
            f"LAST_RX={self.ch2_last_rx}"
        )

        self.get_logger().info(
            f"CH3: TX={self.ch3_tx}, "
            f"RX={self.ch3_rx}, "
            f"MISSED={self.ch3_missed}, "
            f"LAST_RX={self.ch3_last_rx}"
        )

        self.get_logger().info("========================================")


# ====================================================================
# MAIN
# ====================================================================
def main(args=None):

    rclpy.init(args=args)

    node = ThreeChannelContinuousTest()

    try:

        rclpy.spin(node)

    except KeyboardInterrupt:

        pass

    finally:

        node.print_statistics()

        node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
