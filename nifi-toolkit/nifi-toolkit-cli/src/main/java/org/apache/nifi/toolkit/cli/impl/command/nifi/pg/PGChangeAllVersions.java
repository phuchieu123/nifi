/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ProcessGroupsResult;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ProcessGroupsVersionChangeResult;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ChangeVersionResult;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Command to change the version of a version controlled process group.
 */
public class PGChangeAllVersions extends AbstractNiFiCommand<ProcessGroupsVersionChangeResult> {

    public PGChangeAllVersions() {
        super("pg-change-all-versions", ProcessGroupsVersionChangeResult.class);
    }

  @Override
    public String getDescription() {
        return "Thay đổi phiên bản cho tất cả các phiên bản nhóm quy trình được kiểm soát cho một ID luồng nhất định. "
                + "Điều này có thể được sử dụng để nâng cấp tất cả các phiên bản của một luồng đã được phiên bản lên phiên bản mới, hoặc "
                + "hoàn nguyên về phiên bản trước đó. Nếu không có phiên bản nào được chỉ định, phiên bản mới nhất sẽ được sử dụng. "
                + "Nếu không có ID nhóm quy trình nào được cung cấp, nhóm quy trình gốc sẽ được sử dụng để đệ quy "
                + "tìm kiếm tất cả các phiên bản của ID Luồng. Có thể buộc hoạt động đệ quy "
                + "và không dừng hoạt động trong trường hợp việc nâng cấp một nhóm quy trình không thành công.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.FORCE.createOption());
    }

    @Override
    public ProcessGroupsVersionChangeResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final FlowClient flowClient = client.getFlowClient();
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);

        // get the optional id of the parent PG, otherwise fallback to the root group
        String parentPgId = getArg(properties, CommandOption.PG_ID);
        if (StringUtils.isBlank(parentPgId)) {
            parentPgId = flowClient.getRootGroupId();
        }

        final PGList doPGList = new PGList();
        final List<ProcessGroupDTO> pgList = new ArrayList<ProcessGroupDTO>();
        recursivePGList(pgList, doPGList, client, properties, parentPgId);

        final PGChangeVersion doPGChangeVersion = new PGChangeVersion();

        // new version, if specified in the arguments
        Integer newVersion = getIntArg(properties, CommandOption.FLOW_VERSION);

        // force operation, if specified in the arguments
        final boolean forceOperation = properties.containsKey(CommandOption.FORCE.getLongName());

        final List<ProcessGroupDTO> processGroups = new ArrayList<>();
        final Map<String, ChangeVersionResult> changeVersionResults = new HashMap<String, ChangeVersionResult>();

        for (final ProcessGroupDTO pgDTO : pgList) {
            final VersionControlInformationEntity entity = client.getVersionsClient().getVersionControlInfo(pgDTO.getId());

            if(entity.getVersionControlInformation() == null || !entity.getVersionControlInformation().getFlowId().equals(flowId)) {
                continue; // the process group is not version controlled or does not match the provided Flow ID
            }

            if(newVersion == null) {
                newVersion = doPGChangeVersion.getLatestVersion(client, entity.getVersionControlInformation());
            }

            processGroups.add(pgDTO);

            final Integer previousVersion = pgDTO.getVersionControlInformation().getVersion();
            if (previousVersion == newVersion) {
                changeVersionResults.put(pgDTO.getId(), new ChangeVersionResult(newVersion, newVersion, "Process group already at desired version"));
                continue; // Process group already at desired version
            }

            try {
                doPGChangeVersion.changeVersion(client, entity, newVersion, pgDTO.getId(), getContext());
                changeVersionResults.put(pgDTO.getId(), new ChangeVersionResult(previousVersion, newVersion, "SUCCESS"));
            } catch (Exception e) {
                changeVersionResults.put(pgDTO.getId(), new ChangeVersionResult(previousVersion, null, e.getMessage()));
                if (forceOperation) {
                    continue;
                } else {
                    e.printStackTrace();
                    break;
                }
            }
        }

        return new ProcessGroupsVersionChangeResult(getResultType(properties), processGroups, changeVersionResults);
    }

    private void recursivePGList(final List<ProcessGroupDTO> pgList, final PGList doPGList, final NiFiClient client,
            final Properties properties, final String pgId) throws NiFiClientException, IOException {
        final ProcessGroupsResult result = doPGList.getList(client, properties, pgId);
        for(ProcessGroupDTO pgDTO : result.getResult()) {
            pgList.add(pgDTO);
            recursivePGList(pgList, doPGList, client, properties, pgDTO.getId());
        }
    }

}
