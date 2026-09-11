defmodule FreshTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Fresh.TestClient
  alias Fresh.TestServer

  setup_all do
    TestServer.start(8080)
    :ok
  end

  describe "Connecting to:" do
    setup do
      [welcome: "hello", pid: self(), opts: [error_logging: false, info_logging: false]]
    end

    test "Non-Existing Domain", state do
      TestClient.start(uri: "wss://none.bun.rip", state: state, opts: state[:opts])

      assert_receive {:error, {:connecting_failed, %Mint.TransportError{reason: :nxdomain}}}
    end

    test "Echo Server", state do
      TestClient.start_link(
        uri: "ws://localhost:8080/websocket",
        state: state,
        opts: state[:opts]
      )

      assert_receive {:data, {:text, "hello"}}
    end

    test "Echo Server with Registered Process", state do
      TestClient.start_link(
        uri: "ws://localhost:8080/websocket",
        state: state,
        opts: state[:opts] ++ [name: {:local, :client}]
      )

      assert_receive {:data, {:text, "hello"}}

      Fresh.send(:client, {:text, "hi :)"})
      assert_receive {:data, {:text, "hi :)"}}
    end

    test "Send Frame Before WebSocket Handshake Completes", state do
      {:ok, pid} =
        TestClient.start(
          uri: "ws://localhost:8080/websocket",
          state: state,
          opts: state[:opts]
        )

      # queued behind the handshake instead of crashing the connection process
      Fresh.send(pid, {:text, "queued before connect"})

      assert_receive {:data, {:text, "queued before connect"}}
      assert_receive {:data, {:text, "hello"}}
      assert Process.alive?(pid)
    end

    test "Send Multiple Frames Before WebSocket Handshake Completes", state do
      {:ok, pid} =
        TestClient.start(
          uri: "ws://localhost:8080/websocket",
          state: state,
          opts: state[:opts]
        )

      Fresh.send(pid, {:text, "first"})
      Fresh.send(pid, {:text, "second"})
      Fresh.send(pid, {:text, "third"})

      assert_receive {:data, {:text, "first"}}
      assert_receive {:data, {:text, "second"}}
      assert_receive {:data, {:text, "third"}}
      assert_receive {:data, {:text, "hello"}}
      assert Process.alive?(pid)
    end
  end

  describe "Test Echo Server:" do
    setup do
      state = [
        welcome: "hi!",
        pid: self(),
        opts: [
          error_logging: false,
          info_logging: false,
          ping_interval: 5_000
        ]
      ]

      {:ok, pid} =
        TestClient.start(
          uri: "ws://localhost:8080/websocket",
          state: state,
          opts: state[:opts]
        )

      receive do
        {:data, {:text, "hi!"}} ->
          [pid: pid]
      end
    end

    test "Send Text Frame", %{pid: pid} do
      Fresh.send(pid, {:text, "how are you?"})
      assert_receive {:data, {:text, "how are you?"}}
    end

    test "Send Binary Frame", %{pid: pid} do
      Fresh.send(pid, {:binary, <<13, 37>>})
      assert_receive {:data, {:binary, <<13, 37>>}}
    end

    test "Send Ping Frame", %{pid: pid} do
      Fresh.send(pid, {:ping, "wow"})
      assert_receive {:control, {:ping, "wow"}}
    end

    test "Send Pong Frame", %{pid: pid} do
      Fresh.send(pid, {:pong, "lol"})
      assert_receive {:control, {:pong, "lol"}}
    end

    test "Send Multiple Frame", %{pid: pid} do
      Fresh.send(pid, {:text, "ur"})
      Fresh.send(pid, {:binary, "cool"})
      Fresh.send(pid, {:ping, ":)"})

      assert_receive {:data, {:text, "ur"}}
      assert_receive {:data, {:binary, "cool"}}
      assert_receive {:control, {:ping, ":)"}}
    end

    test "Send Message to Process", %{pid: pid} do
      send(pid, {:send_frame, {:binary, <<1, 2, 3>>}})
      assert_receive {:data, {:binary, <<1, 2, 3>>}}

      send(pid, :another)
      assert_receive {:info, :another}
    end

    test "Close Connection with Close Frame", %{pid: pid} do
      Fresh.send(pid, {:close, 1002, ""})
      assert_receive {:close, 1000, ""}
    end

    test "Close Connection from Callback", %{pid: pid} do
      assert Fresh.open?(pid) == true

      Fresh.send(pid, {:text, "try closing from callback"})

      assert_receive {:close, 1000, ""}
      assert Fresh.open?(pid) == false
    end

    test "Close Connection with Text Frame", %{pid: pid} do
      Fresh.send(pid, {:text, "close it!"})

      assert_receive {:close, 1013, "yessir"}
      assert_receive {:terminate, :shutdown}
    end

    test "Close Connection and Reconnect", %{pid: pid} do
      Fresh.close(pid, 1002, "")

      assert_receive {:close, 1000, ""}
      assert_receive {:data, {:text, "hi!"}}

      Fresh.send(pid, {:binary, "hello once again!"})
      assert_receive {:data, {:binary, "hello once again!"}}
    end

    test "Wait for Ping", _ do
      assert_receive {:control, {:ping, ""}}, 10_000
    end
  end

  describe "Error Logging:" do
    test "Casting Failure Logs the Real Reason Instead of the Connection Struct" do
      {:ok, pid} =
        TestClient.start(
          uri: "ws://localhost:8080/websocket",
          state: [welcome: "hi", pid: self()],
          opts: [error_logging: true, info_logging: true]
        )

      assert_receive {:data, {:text, "hi"}}

      # simulate the underlying socket dying between frames
      {:connected, %Fresh.Connection{connection: %{socket: socket}}} = :sys.get_state(pid)
      :gen_tcp.close(socket)

      log =
        capture_log(fn ->
          Fresh.send(pid, {:text, "this will fail to send"})
          assert_receive {:error, {:casting_failed, _reason}}
        end)

      assert log =~ "Casting message failed:"
      assert log =~ "ws://localhost:8080/websocket"
      refute log =~ "%Fresh.Connection{"
    end

    test "Closed Transport Errors Log at Info Level" do
      {:ok, pid} =
        TestClient.start(
          uri: "ws://localhost:8080/websocket",
          state: [welcome: "hi", pid: self()],
          opts: [error_logging: false, info_logging: true]
        )

      assert_receive {:data, {:text, "hi"}}

      {:connected, %Fresh.Connection{connection: %{socket: socket}}} = :sys.get_state(pid)
      :gen_tcp.close(socket)

      log =
        capture_log(fn ->
          Fresh.send(pid, {:text, "this will fail to send"})
          assert_receive {:error, {:casting_failed, %Mint.TransportError{reason: :closed}}}
        end)

      assert log =~ "[info]"
      assert log =~ "Casting message failed:"
    end

    test "Established Connection Logs Include the Endpoint" do
      log =
        capture_log(fn ->
          TestClient.start(
            uri: "ws://localhost:8080/websocket",
            state: [welcome: "hi", pid: self()],
            opts: [error_logging: false, info_logging: true]
          )

          assert_receive {:data, {:text, "hi"}}
        end)

      assert log =~ "WebSocket connection established"
      assert log =~ "ws://localhost:8080/websocket"
    end

    test "Disabled Logging Options Produce No Log Output" do
      log =
        capture_log(fn ->
          {:ok, pid} =
            TestClient.start(
              uri: "ws://localhost:8080/websocket",
              state: [welcome: "hi", pid: self()],
              opts: [error_logging: false, info_logging: false]
            )

          assert_receive {:data, {:text, "hi"}}

          {:connected, %Fresh.Connection{connection: %{socket: socket}}} = :sys.get_state(pid)
          :gen_tcp.close(socket)

          Fresh.send(pid, {:text, "this will fail to send"})
          assert_receive {:error, {:casting_failed, _reason}}
        end)

      assert log == ""
    end
  end
end
