import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mwaysolutions.bluerange.ApiClient;
import com.mwaysolutions.bluerange.ApiException;
import com.mwaysolutions.bluerange.api.DevicesApi;
import com.mwaysolutions.bluerange.api.IotActuatorApi;
import com.mwaysolutions.bluerange.model.ActuatorDataRequest;
import com.mwaysolutions.bluerange.model.ActuatorInfoQuery;
import com.mwaysolutions.bluerange.model.ActuatorInfoResult;
import com.mwaysolutions.bluerange.model.Device;
import com.mwaysolutions.bluerange.model.Filter;
import com.mwaysolutions.bluerange.model.LogOpFilter;
import com.mwaysolutions.bluerange.model.StringEnumFilter;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    private static final String BLUERANGE_USER_ACCESS_TOKEN = System.getenv("BLUERANGE_USER_ACCESS_TOKEN");
    private static final String BLUERANGE_TENANT_ORGANIZATION_UUID = System.getenv("BLUERANGE_TENANT_ORGANIZATION_UUID");

    public static void main(final String[] args) {
        final String deviceId = argAt(args, 0);
        final String actuatorType = argAt(args, 1);
        final String actuatorValue = argAt(args, 2);
        try {
            final ApiClient apiClient = new ApiClient();
            apiClient.setRequestInterceptor(Main::authorizeRequest);
            apiClient.setResponseInterceptor(Main::traceResponse);

            final DevicesApi devicesApi = new DevicesApi(apiClient);
            if (deviceId == null) {
                final List<Device> devices = devicesApi.getDevices(null, null,
                        "+" + Device.JSON_PROPERTY_DEVICE_ID, null,
                        List.of(Device.JSON_PROPERTY_UUID, Device.JSON_PROPERTY_DEVICE_ID),
                        true, false).getResults();
                LOGGER.info("Following devices are accessible: " + devices.stream()
                        .map(Device::getDeviceId)
                        .filter(Objects::nonNull)
                        .collect(Collectors.joining(", ")));
                return;
            }

            final List<Device> devices = devicesApi.getDevices(null, null,
                    null, filterToString(apiClient, new LogOpFilter()
                            .operation(LogOpFilter.OperationEnum.AND)
                            .addFiltersItem(new StringEnumFilter()
                                    .fieldName(Device.JSON_PROPERTY_STATUS)
                                    .values(EnumSet.complementOf(EnumSet.of(
                                            Device.StatusEnum.ENROLLMENT_PENDING,
                                            Device.StatusEnum.DELETION_PENDING, Device.StatusEnum.DELETED,
                                            Device.StatusEnum.WITHDRAW_PENDING, Device.StatusEnum.WITHDRAWN
                                    )).stream()
                                            .map(Device.StatusEnum::getValue)
                                            .collect(Collectors.toList()))
                                    .type("stringEnum")
                            )
                            .addFiltersItem(new StringEnumFilter()
                                    .fieldName(Device.JSON_PROPERTY_DEVICE_ID)
                                    .addValuesItem(deviceId)
                                    .type("stringEnum")
                            )
                            .type("logOp")),
                    null,
                    true, false).getResults();
            if (devices.isEmpty()) {
                throw new RuntimeException("no device");
            }
            if (devices.size() != 1) {
                throw new RuntimeException(devices.size() + " devices");
            }
            final Device device = devices.get(0);
            LOGGER.info("Device " + device.getName() + " (" + device.getUuid() + "/" + device.getDeviceId() + ")");

            final IotActuatorApi iotActuatorApi = new IotActuatorApi(apiClient);
            if (actuatorType == null) {
                final List<ActuatorInfoResult> actuatorInfoResults = iotActuatorApi.queryActuatorInfo(new ActuatorInfoQuery()
                        .addDeviceUuidsItem(device.getUuid())
                ).getResults();
                LOGGER.info("Known actuator types are: " + actuatorInfoResults.stream()
                        .map(ActuatorInfoResult::getType)
                        .distinct()
                        .sorted()
                        .collect(Collectors.joining(", ")));
                return;
            }

            final ActuatorDataRequest actuatorDataRequest = new ActuatorDataRequest()
                    .addDeviceUuidsItem(device.getUuid())
                    .type(actuatorType)
                    .index(0);
            if (actuatorValue != null) {
                actuatorDataRequest.setValue(apiClient.getObjectMapper().readValue(actuatorValue, JsonNode.class));
            }
            LOGGER.info("Actuator body payload: " + apiClient.getObjectMapper().writeValueAsString(actuatorDataRequest));
            iotActuatorApi.actionActuatorData(actuatorDataRequest);
        } catch(final ApiException x) {
            if (x.getCode() == 401 && BLUERANGE_USER_ACCESS_TOKEN == null) {
                LOGGER.severe("You must provide an access token in environment variable BLUERANGE_USER_ACCESS_TOKEN!");
            } else {
                LOGGER.log(Level.SEVERE, x.getResponseBody(), x);
            }
            System.exit(x.getCode());
        } catch(final Exception x) {
            LOGGER.log(Level.SEVERE, x.getMessage(), x);
            System.exit(1);
        }
    }

    private static String argAt(final String[] args, final int index) {
        return args.length > index ? args[index] : null;
    }

    private static void authorizeRequest(final HttpRequest.Builder builder) {
        if (BLUERANGE_USER_ACCESS_TOKEN != null) {
            builder.setHeader("X-User-Access-Token", BLUERANGE_USER_ACCESS_TOKEN);
        }
        if (BLUERANGE_TENANT_ORGANIZATION_UUID != null) {
            builder.uri(queryParam(builder.build().uri(), "tenantOrganizationUuid", BLUERANGE_TENANT_ORGANIZATION_UUID));
        }
    }

    private static void traceResponse(final HttpResponse<InputStream> response) {
        LOGGER.info(() -> response.request().method() + " " + response.request().uri() + ": " + response.statusCode());
    }

    private static URI queryParam(final URI uri, final String key, final String value) {
        String query = uri.getQuery();
        if (query == null) {
            query = "";
        } else {
            query += "&";
        }
        query += key;
        query += "=";
        query += value;

        try {
            return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), query, uri.getFragment());
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(uri.toString(), x);
        }
    }

    private static String filterToString(final ApiClient apiClient, final Filter filter) throws JsonProcessingException {
        return apiClient.getObjectMapper().writeValueAsString(filter);
    }
}
