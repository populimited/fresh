defmodule Fresh.Log do
  @moduledoc false

  alias Fresh.Option

  require Logger

  def log(level, message, extra, data) do
    formatted =
      message
      |> message_to_string(extra)
      |> add_title(data.uri)

    case level do
      :info ->
        if Option.info_logging(data.opts) do
          Logger.info(formatted)
        end

      :error ->
        if Option.error_logging(data.opts) do
          Logger.error(formatted)
        end
    end
  end

  defp add_title(message, uri) do
    "(Fresh) [#{uri}] #{message}"
  end

  defp message_to_string(:established, _extra),
    do: "WebSocket connection established"

  defp message_to_string(:connecting_failed, reason),
    do: "Connecting to host failed: #{inspect(reason)}"

  defp message_to_string(:upgrading_failed, reason),
    do: "Upgrading connection to WebSocket failed: #{inspect(reason)}"

  defp message_to_string(:streaming_failed, reason),
    do: "Creating new stream for connection failed: #{inspect(reason)}"

  defp message_to_string(:establishing_failed, reason),
    do: "Establishing WebSocket connection failed: #{inspect(reason)}"

  defp message_to_string(:processing_failed, reason),
    do: "Processing upgrade request failed: #{inspect(reason)}"

  defp message_to_string(:decoding_failed, reason),
    do: "Decoding message failed: #{inspect(reason)}"

  defp message_to_string(:encoding_failed, reason),
    do: "Encoding message failed: #{inspect(reason)}"

  defp message_to_string(:casting_failed, reason),
    do: "Casting message failed: #{inspect(reason)}"

  defp message_to_string(:dropping, {code, reason}),
    do: "WebSocket connection is being closed with code #{code}: #{reason}"
end
