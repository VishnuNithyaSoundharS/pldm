#include "sensor_manager.hpp"

#include "manager.hpp"
#include "terminus_manager.hpp"

#include <phosphor-logging/lg2.hpp>

#include <exception>

namespace pldm
{
namespace platform_mc
{

SensorManager::SensorManager(sdeventplus::Event& event,
                             TerminusManager& terminusManager,
                             TerminiMapper& termini, Manager* manager) :
    event(event), terminusManager(terminusManager), termini(termini),
    pollingTime(SENSOR_POLLING_TIME), manager(manager)
{}

void SensorManager::startPolling(pldm_tid_t tid)
{
    if (!termini.contains(tid))
    {
        return;
    }

    /* tid already initializes roundRobinSensors list */
    if (sensorPollTimers.contains(tid))
    {
        lg2::info("Terminus ID {TID}: sensor poll timer already exists.", "TID",
                  tid);
        return;
    }

    roundRobinSensorItMap[tid] = 0;

    updateAvailableState(tid, true);

    sensorPollTimers[tid] = std::make_unique<sdbusplus::Timer>(
        event.get(), [this, tid] { this->doSensorPolling(tid); });

    startSensorPollTimer(tid);
}

void SensorManager::startSensorPollTimer(pldm_tid_t tid)
{
    try
    {
        if (sensorPollTimers[tid] && !sensorPollTimers[tid]->isRunning())
        {
            sensorPollTimers[tid]->start(
                duration_cast<std::chrono::milliseconds>(
                    std::chrono::milliseconds(pollingTime)),
                true);
        }
    }
    catch (const std::exception& e)
    {
        lg2::error(
            "Terminus ID {TID}: Failed to start sensor polling timer. Exception: {EXCEPTION}",
            "TID", tid, "EXCEPTION", e);
        return;
    }
}

void SensorManager::disableTerminusSensors(pldm_tid_t tid)
{
    if (!termini.contains(tid))
    {
        return;
    }

    // numeric sensor
    auto terminus = termini[tid];
    if (!terminus)
    {
        return;
    }

    for (auto& sensor : terminus->numericSensors)
    {
        sensor->updateReading(true, false,
                              std::numeric_limits<double>::quiet_NaN());
    }
}

void SensorManager::stopPolling(pldm_tid_t tid)
{
    /* Stop polling timer */
    if (sensorPollTimers.contains(tid))
    {
        sensorPollTimers[tid]->stop();
        sensorPollTimers.erase(tid);
    }

    roundRobinSensorItMap.erase(tid);

    if (doSensorPollingTaskHandles.contains(tid))
    {
        auto& [scope, rcOpt] = doSensorPollingTaskHandles[tid];
        scope.request_stop();
        doSensorPollingTaskHandles.erase(tid);
    }

    availableState.erase(tid);
}

void SensorManager::doSensorPolling(pldm_tid_t tid)
{
    auto it = doSensorPollingTaskHandles.find(tid);
    if (it != doSensorPollingTaskHandles.end())
    {
        auto& [scope, rcOpt] = it->second;
        if (!rcOpt.has_value())
        {
            return;
        }
        doSensorPollingTaskHandles.erase(tid);
    }

    auto& [scope, rcOpt] =
        doSensorPollingTaskHandles
            .emplace(std::piecewise_construct, std::forward_as_tuple(tid),
                     std::forward_as_tuple())
            .first->second;
    scope.spawn(
        stdexec::just() | stdexec::let_value([this, &rcOpt,
                                              tid] -> exec::task<void> {
            auto res =
                co_await stdexec::stopped_as_optional(doSensorPollingTask(tid));
            if (res.has_value())
            {
                rcOpt = *res;
            }
            else
            {
                lg2::info("Stopped polling for Terminus ID {TID}", "TID", tid);
                try
                {
                    if (sensorPollTimers.contains(tid) &&
                        sensorPollTimers[tid] &&
                        sensorPollTimers[tid]->isRunning())
                    {
                        sensorPollTimers[tid]->stop();
                    }
                }
                catch (const std::exception& e)
                {
                    lg2::error(
                        "Terminus ID {TID}: Failed to stop polling timer. Exception: {EXCEPTION}",
                        "TID", tid, "EXCEPTION", e);
                }
                rcOpt = PLDM_SUCCESS;
            }
        }),
        exec::default_task_context<void>(exec::inline_scheduler{}));
}

exec::task<int> SensorManager::doSensorPollingTask(pldm_tid_t tid)
{
    uint64_t t0 = 0;
    uint64_t t1 = 0;
    uint64_t elapsed = 0;
    uint64_t pollingTimeInUsec = pollingTime * 1000;
    uint8_t rc = PLDM_SUCCESS;

    lg2::info("Starting sensor polling task for terminus ID {TID}", "TID", tid);

    do
    {
        if ((!sensorPollTimers.contains(tid)) ||
            (sensorPollTimers[tid] && !sensorPollTimers[tid]->isRunning()))
        {
            lg2::info(
                "Sensor poll timer not running for terminus ID {TID}, exiting task",
                "TID", tid);
            co_return PLDM_ERROR;
        }

        sd_event_now(event.get(), CLOCK_MONOTONIC, &t0);
        lg2::info("Polling cycle started at {T0} for terminus ID {TID}", "T0",
                  t0, "TID", tid);

        /**
         * Terminus is not available for PLDM request.
         * The terminus manager will trigger recovery process to recovery the
         * communication between the local terminus and the remote terminus.
         * The sensor polling should be stopped while recovering the
         * communication.
         */
        if (!getAvailableState(tid))
        {
            lg2::info(
                "Terminus ID {TID} is not available for PLDM request from {NOW}.",
                "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
            co_await stdexec::just_stopped();
        }

        if (!termini.contains(tid))
        {
            lg2::info("Terminus ID {TID} not in termini map, returning success",
                      "TID", tid);
            co_return PLDM_SUCCESS;
        }

        lg2::info(
            "Terminus ID {TID} found in termini map, proceeding with polling",
            "TID", tid);
        auto& terminus = termini[tid];
        if (!terminus)
        {
            lg2::info(
                "Terminus ID {TID} does not have a valid Terminus object {NOW}.",
                "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
            co_return PLDM_ERROR;
        }

        if (manager && terminus->pollEvent)
        {
            lg2::info("Polling for platform event for terminus ID {TID}", "TID",
                      tid);
            co_await manager->pollForPlatformEvent(
                tid, terminus->pollEventId, terminus->pollDataTransferHandle);
            lg2::info("Completed platform event polling for terminus ID {TID}",
                      "TID", tid);
        }

        if (manager && (!terminus->pollEvent))
        {
            lg2::info("Polling for OEM platform event for terminus ID {TID}",
                      "TID", tid);
            co_await manager->oemPollForPlatformEvent(tid);
            lg2::info(
                "Completed OEM platform event polling for terminus ID {TID}",
                "TID", tid);
        }

        sd_event_now(event.get(), CLOCK_MONOTONIC, &t1);

        auto& numericSensors = terminus->numericSensors;
        auto toBeUpdated = numericSensors.size();
        lg2::info("Processing {COUNT} sensors for terminus ID {TID}", "COUNT",
                  toBeUpdated, "TID", tid);

        if (!roundRobinSensorItMap.contains(tid))
        {
            lg2::info(
                "Terminus ID {TID} does not have a round robin sensor iteration {NOW}.",
                "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
            co_return PLDM_ERROR;
        }
        auto& sensorIt = roundRobinSensorItMap[tid];

        while (((t1 - t0) < pollingTimeInUsec) && (toBeUpdated > 0))
        {
            if (!getAvailableState(tid))
            {
                lg2::info(
                    "Terminus ID {TID} is not available for PLDM request from {NOW}.",
                    "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
                co_await stdexec::just_stopped();
            }

            if (sensorIt >= numericSensors.size())
            {
                lg2::info(
                    "Wrapping sensor iterator back to 0 for terminus ID {TID}",
                    "TID", tid);
                sensorIt = 0;
            }

            auto sensor = numericSensors[sensorIt];

            sd_event_now(event.get(), CLOCK_MONOTONIC, &t1);
            elapsed = t1 - sensor->timeStamp;
            if ((sensor->updateTime <= elapsed) || (!sensor->timeStamp))
            {
                lg2::info(
                    "Reading sensor {SENSOR} for terminus ID {TID}, elapsed: {ELAPSED}, updateTime: {UPTIME}",
                    "SENSOR", sensor->sensorId, "TID", tid, "ELAPSED", elapsed,
                    "UPTIME", sensor->updateTime);
                if (true || sensor->disabled)
                {
                    lg2::info("Sensor is disabled calling handleSetNumericSensorEnable for sensor {SENSOR}, terminus ID {TID}", "SENSOR", sensor->sensorId, "TID", tid);
                    auto enableRc = co_await handleSetNumericSensorEnable(
                        tid, sensor->sensorId);
                    lg2::info("Returned from handleSetNumericSensorEnable for sensor {SENSOR}, terminus ID {TID} with return code {RC}", "SENSOR", sensor->sensorId, "TID", tid, "RC", enableRc);
                    
                    if (enableRc == PLDM_SUCCESS)
                    {
                        lg2::info("Successfully enabled sensor {SENSOR} for terminus {TID}, will read next cycle", "SENSOR", sensor->sensorId, "TID", tid);
                        sensor->disabled = false;
                    }
                    else
                    {
                        lg2::error("Failed to enable sensor {SENSOR} for terminus {TID}, will retry next cycle", "SENSOR", sensor->sensorId, "TID", tid);
                    }
                    
                    // Skip reading this cycle to give sensor time to stabilize after enable command
                    toBeUpdated--;
                    sensorIt++;
                    continue;
                }

                rc = co_await getSensorReading(sensor);
                if ((!sensorPollTimers.contains(tid)) ||
                    (sensorPollTimers[tid] &&
                     !sensorPollTimers[tid]->isRunning()))
                {
                    lg2::info(
                        "Sensor poll timer stopped during reading for terminus ID {TID}",
                        "TID", tid);
                    co_return PLDM_ERROR;
                }
                sd_event_now(event.get(), CLOCK_MONOTONIC, &t1);
                if (rc == PLDM_SUCCESS)
                {
                    sensor->timeStamp = t1;
                    lg2::info(
                        "Successfully read sensor {SENSOR} for terminus ID {TID}",
                        "SENSOR", sensor->sensorId, "TID", tid);
                }
                else
                {
                    lg2::error(
                        "Failed to get sensor value for terminus {TID}, error: {RC}",
                        "TID", tid, "RC", rc);
                }
            }
            else
            {
                lg2::info(
                    "Skipping sensor {SENSOR} for terminus ID {TID}, not due for update (elapsed: {ELAPSED}, updateTime: {UPTIME})",
                    "SENSOR", sensor->sensorId, "TID", tid, "ELAPSED", elapsed,
                    "UPTIME", sensor->updateTime);
            }

            toBeUpdated--;
            sensorIt++;
            lg2::info(
                "Remaining sensors to process: {REMAINING}, current iterator: {IT} for terminus ID {TID}",
                "REMAINING", toBeUpdated, "IT", sensorIt, "TID", tid);

            sd_event_now(event.get(), CLOCK_MONOTONIC, &t1);
        }

        sd_event_now(event.get(), CLOCK_MONOTONIC, &t1);
        lg2::info(
            "Polling cycle completed for terminus ID {TID}, duration: {DURATION} usec, pollingTimeInUsec: {POLLTIME}",
            "TID", tid, "DURATION", (t1 - t0), "POLLTIME", pollingTimeInUsec);
    } while ((t1 - t0) >= pollingTimeInUsec);

    lg2::info(
        "Exiting do-while loop for terminus ID {TID}, cycle duration {DURATION} < pollingTimeInUsec {POLLTIME}",
        "TID", tid, "DURATION", (t1 - t0), "POLLTIME", pollingTimeInUsec);

    lg2::info(
        "Sensor polling task completed successfully for terminus ID {TID}",
        "TID", tid);
    co_return PLDM_SUCCESS;
}

exec::task<int> SensorManager::getSensorReading(
    std::shared_ptr<NumericSensor> sensor)
{
    lg2::info(
        "vdbg: getSensorReading called for terminus ID {TID}, sensor Id {ID}.",
        "TID", sensor->tid, "ID", sensor->sensorId);
    if (!sensor)
    {
        lg2::error("Call `getSensorReading` with null `sensor` pointer.");
        co_return PLDM_ERROR_INVALID_DATA;
    }

    // if (sensor->disabled)
    // {
    //     lg2::info(
    //         "Sensor is disabled calling handleSetNumericSensorEnable for
    //         sensor {SENSOR}, terminus ID {TID}", "SENSOR", sensor->sensorId,
    //         "TID", sensor->tid);
    //     auto rc = co_await handleSetNumericSensorEnable(sensor->tid,
    //                                                     sensor->sensorId);
    //     lg2::info(
    //         "Returned from handleSetNumericSensorEnable for sensor {SENSOR},
    //         terminus ID {TID} with return code {RC}", "SENSOR",
    //         sensor->sensorId, "TID", sensor->tid, "RC", rc);
    // }

    auto tid = sensor->tid;
    auto sensorId = sensor->sensorId;
    Request request(sizeof(pldm_msg_hdr) + PLDM_GET_SENSOR_READING_REQ_BYTES);
    auto requestMsg = new (request.data()) pldm_msg;
    auto rc = encode_get_sensor_reading_req(0, sensorId, false, requestMsg);
    if (rc)
    {
        lg2::error(
            "Failed to encode request GetSensorReading for terminus ID {TID}, sensor Id {ID}, error {RC}.",
            "TID", tid, "ID", sensorId, "RC", rc);
        co_return rc;
    }
    lg2::info(
        "Encoded GetSensorReading request for terminus ID {TID}, sensor Id {ID}",
        "TID", tid, "ID", sensorId);

    if (!getAvailableState(tid))
    {
        lg2::info(
            "Terminus ID {TID} is not available for PLDM request from {NOW}.",
            "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
        co_await stdexec::just_stopped();
    }

    lg2::info(
        "Sending GetSensorReading request for terminus ID {TID}, sensor Id {ID}",
        "TID", tid, "ID", sensorId);
    const pldm_msg* responseMsg = nullptr;
    size_t responseLen = 0;
    rc = co_await terminusManager.sendRecvPldmMsg(tid, request, &responseMsg,
                                                  &responseLen);
    if (rc)
    {
        lg2::error(
            "Failed to send GetSensorReading message for terminus {TID}, sensor Id {ID}, error {RC}",
            "TID", tid, "ID", sensorId, "RC", rc);
        co_return rc;
    }

    if ((!sensorPollTimers.contains(tid)) ||
        (sensorPollTimers[tid] && !sensorPollTimers[tid]->isRunning()))
    {
        lg2::info(
            "Sensor poll timer stopped after receiving response for terminus ID {TID}, sensor Id {ID}",
            "TID", tid, "ID", sensorId);
        co_return PLDM_ERROR;
    }

    lg2::info(
        "Received GetSensorReading response for terminus ID {TID}, sensor Id {ID}, length: {LEN}",
        "TID", tid, "ID", sensorId, "LEN", responseLen);
    uint8_t completionCode = PLDM_SUCCESS;
    uint8_t sensorDataSize = PLDM_SENSOR_DATA_SIZE_SINT32;
    uint8_t sensorOperationalState = 0;
    uint8_t sensorEventMessageEnable = 0;
    uint8_t presentState = 0;
    uint8_t previousState = 0;
    uint8_t eventState = 0;
    union_sensor_data_size presentReading;
    rc = decode_get_sensor_reading_resp(
        responseMsg, responseLen, &completionCode, &sensorDataSize,
        &sensorOperationalState, &sensorEventMessageEnable, &presentState,
        &previousState, &eventState,
        reinterpret_cast<uint8_t*>(&presentReading));
    if (rc)
    {
        lg2::error(
            "Failed to decode response GetSensorReading for terminus ID {TID}, sensor Id {ID}, error {RC}.",
            "TID", tid, "ID", sensorId, "RC", rc);
        sensor->handleErrGetSensorReading();
        co_return rc;
    }

    if (completionCode != PLDM_SUCCESS)
    {
        lg2::error(
            "Error : GetSensorReading for terminus ID {TID}, sensor Id {ID}, complete code {CC}.",
            "TID", tid, "ID", sensorId, "CC", completionCode);
        co_return completionCode;
    }

    lg2::info(
        "Decoded GetSensorReading response for terminus ID {TID}, sensor Id {ID}, operational state: {STATE}",
        "TID", tid, "ID", sensorId, "STATE", sensorOperationalState);
    double value = std::numeric_limits<double>::quiet_NaN();
    switch (sensorOperationalState)
    {
        case PLDM_SENSOR_ENABLED:
            lg2::info("Sensor {SENSOR} is enabled for terminus ID {TID}",
                      "SENSOR", sensorId, "TID", tid);
            lg2::info(
                "Marking sensor {SENSOR} as enabled for terminus ID {TID}",
                "SENSOR", sensorId, "TID", tid);
            sensor->disabled = false;
            break;
        case PLDM_SENSOR_DISABLED:
            lg2::info(
                "Sensor {SENSOR} is disabled for terminus ID {TID}, attempting to enable",
                "SENSOR", sensorId, "TID", tid);
            sensor->updateReading(false, true, value);
            lg2::info(
                "Marking sensor {SENSOR} as disabled for terminus ID {TID}",
                "SENSOR", sensorId, "TID", tid);
            sensor->disabled = true;
            lg2::info(
                "Marked sensor {SENSOR} as disabled for terminus ID {TID}",
                "SENSOR", sensorId, "TID", tid);
            co_return PLDM_SENSOR_DISABLED;
        case PLDM_SENSOR_FAILED:
            lg2::info(
                "Sensor {SENSOR} is in failed state for terminus ID {TID}",
                "SENSOR", sensorId, "TID", tid);
            sensor->updateReading(true, false, value);
            co_return completionCode;
        case PLDM_SENSOR_UNAVAILABLE:
        default:
            lg2::info("Sensor {SENSOR} is unavailable for terminus ID {TID}",
                      "SENSOR", sensorId, "TID", tid);
            sensor->updateReading(false, false, value);
            co_return completionCode;
    }

    lg2::info(
        "Parsing sensor data for terminus ID {TID}, sensor Id {ID}, data size: {SIZE}",
        "TID", tid, "ID", sensorId, "SIZE", sensorDataSize);
    switch (sensorDataSize)
    {
        case PLDM_SENSOR_DATA_SIZE_UINT8:
            value = static_cast<double>(presentReading.value_u8);
            break;
        case PLDM_SENSOR_DATA_SIZE_SINT8:
            value = static_cast<double>(presentReading.value_s8);
            break;
        case PLDM_SENSOR_DATA_SIZE_UINT16:
            value = static_cast<double>(presentReading.value_u16);
            break;
        case PLDM_SENSOR_DATA_SIZE_SINT16:
            value = static_cast<double>(presentReading.value_s16);
            break;
        case PLDM_SENSOR_DATA_SIZE_UINT32:
            value = static_cast<double>(presentReading.value_u32);
            break;
        case PLDM_SENSOR_DATA_SIZE_SINT32:
            value = static_cast<double>(presentReading.value_s32);
            break;
        default:
            value = std::numeric_limits<double>::quiet_NaN();
            break;
    }

    sensor->updateReading(true, true, value);
    lg2::info("Sensor reading for terminus ID {TID}, sensor Id {ID} is {VALUE}",
              "TID", tid, "ID", sensorId, "VALUE", value);
    co_return completionCode;
}

exec::task<int> SensorManager::setNumericSensorEnable(
    pldm_tid_t tid, SensorID sensorId, uint8_t sensorOperationalState,
    uint8_t sensorEventMessageEnable)
{
    lg2::info(
        "setNumericSensorEnable called for terminus ID {TID}, sensor ID {SENSOR}, state: {STATE}",
        "TID", tid, "SENSOR", sensorId, "STATE", sensorOperationalState);
    Request request(
        sizeof(pldm_msg_hdr) + PLDM_SET_NUMERIC_SENSOR_ENABLE_REQ_BYTES);
    auto requestMsg1 = new (request.data()) pldm_msg;
    auto rc = encode_set_numeric_sensor_enable_req(
        0, sensorId, sensorOperationalState, sensorEventMessageEnable,
        requestMsg1);
    if (rc)
    {
        lg2::error(
            "Failed to encode request SetNumericSensorEnable for terminus ID {TID}, sensor ID {SENSOR}, error {RC}",
            "TID", tid, "SENSOR", sensorId, "RC", rc);
        co_return rc;
    }
    lg2::info(
        "Encoded SetNumericSensorEnable request for terminus ID {TID}, sensor ID {SENSOR}",
        "TID", tid, "SENSOR", sensorId);

    if (!getAvailableState(tid))
    {
        lg2::info(
            "Terminus ID {TID} is not available for PLDM request from {NOW}.",
            "TID", tid, "NOW", pldm::utils::getCurrentSystemTime());
        co_await stdexec::just_stopped();
    }

    lg2::info(
        "Sending SetNumericSensorEnable request for terminus ID {TID}, sensor ID {SENSOR}",
        "TID", tid, "SENSOR", sensorId);
    const pldm_msg* responseMsg = nullptr;
    size_t responseLen = 0;
    rc = PLDM_SUCCESS; //co_await terminusManager.sendRecvPldmMsg(tid, request, &responseMsg,
                         //                         &responseLen);
    if (rc)
    {
        lg2::error(
            "Failed to send SetNumericSensorEnable message for terminus {TID}, sensor ID {SENSOR}, error {RC}",
            "TID", tid, "SENSOR", sensorId, "RC", rc);
        co_return rc;
    }

    lg2::info(
        "Received SetNumericSensorEnable response for terminus ID {TID}, sensor ID {SENSOR}, length: {LEN}",
        "TID", tid, "SENSOR", sensorId, "LEN", responseLen);
    uint8_t completionCode = PLDM_SUCCESS;
    rc = decode_set_numeric_sensor_enable_resp(responseMsg, responseLen,
                                               &completionCode);
    if (rc)
    {
        lg2::error(
            "Failed to decode response SetNumericSensorEnable for terminus ID {TID}, sensor ID {SENSOR}, error {RC}",
            "TID", tid, "SENSOR", sensorId, "RC", rc);
        co_return rc;
    }

    if (completionCode != PLDM_SUCCESS)
    {
        lg2::error(
            "Error : SetNumericSensorEnable for terminus ID {TID}, sensor ID {SENSOR}, complete code {CC}.",
            "TID", tid, "SENSOR", sensorId, "CC", completionCode);
        co_return completionCode;
    }

    lg2::info("Successfully enabled sensor {SENSOR} for terminus ID {TID}",
              "SENSOR", sensorId, "TID", tid);
    co_return completionCode;
}

exec::task<int> SensorManager::handleSetNumericSensorEnable(pldm_tid_t tid,
                                                            SensorID sensorId)
{
    if (!termini.contains(tid))
    {
        lg2::error("Terminus {TID} not found", "TID", tid);
        co_return PLDM_ERROR;
    }

    auto& terminus = termini[tid];
    if (!terminus)
    {
        lg2::error("Terminus {TID} has invalid terminus object", "TID", tid);
        co_return PLDM_ERROR;
    }

    // Check if terminus supports SetNumericSensorEnable command
    if (!terminus->doesSupportCommand(PLDM_PLATFORM,
                                      PLDM_SET_NUMERIC_SENSOR_ENABLE))
    {
        lg2::debug(
            "Terminus {TID} does not support SetNumericSensorEnable command",
            "TID", tid);
        co_return PLDM_ERROR;
    }

    // Call the lower-level function to enable the specific sensor
    auto rc = co_await setNumericSensorEnable(
        tid, sensorId, PLDM_SENSOR_ENABLED, PLDM_EVENT_MESSAGE_NO_CHANGE);

    if (rc != PLDM_SUCCESS)
    {
        lg2::error(
            "Failed to enable numeric sensor {SENSOR} for terminus {TID}, error {RC}",
            "SENSOR", sensorId, "TID", tid, "RC", rc);
    }

    co_return rc;
}

} // namespace platform_mc
} // namespace pldm
