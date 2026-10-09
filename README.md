# mqtt-lab

Runnable examples for the **Dev Skills Unlock** MQTT series. One fictional platform, *Last Mile*
(courier phones on flaky 4G, dark-store edge gateways, a .NET backend), grows one episode at a time.

| Episode | Branch | Folder |
|---|---|---|
| E1 — Stop Using HTTP Webhooks for Devices | `e1-start` | `e1/` |

Requires the .NET 10 SDK and Podman (or Docker).

```powershell
# Broker (lab only: anonymous, plaintext)
podman run -d --name lastmile-broker -p 1883:1883 `
  -v ${PWD}/broker/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro `
  public.ecr.aws/docker/library/eclipse-mosquitto:2
# or: podman compose up -d / docker compose up -d

dotnet run --project e1/LastMile.E1 -- bytes    # HTTP webhook vs MQTT, bytes on the wire
dotnet run --project e1/LastMile.E1 -- tunnel   # courier loses signal, ops gets the Last Will
```

`MQTT_HOST` overrides the broker host (default `127.0.0.1`).

## E1 — what you should see

`bytes` puts a byte-counting TCP proxy in front of a local HTTP endpoint and in front of the
broker, then sends the same 71-byte JSON location 100 times. Plaintext, HTTP/1.1 keep-alive
(the most charitable HTTP there is), so the difference is protocol overhead only.

`tunnel` drives a courier into a 15-second tunnel. The proxy stops forwarding without closing
anything, like a dead radio link. The broker notices the missing keep-alive and publishes the
courier's Last Will; a dispatcher who connects at the end gets status and last location from
retained messages before anyone publishes.

Locations sent inside the tunnel are lost. That is deliberate — E2 fixes it.
