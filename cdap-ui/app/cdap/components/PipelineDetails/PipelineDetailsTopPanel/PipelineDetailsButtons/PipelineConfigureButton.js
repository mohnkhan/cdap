/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import PipelineConfigurations from 'components/PipelineConfigurations';
import {MyPreferenceApi} from 'api/preference';
import {getCurrentNamespace} from 'services/NamespaceStore';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {getMacrosResolvedByPrefs} from 'components/PipelineConfigurations/Store/ActionCreator';
import T from 'i18n-react';
import PipelineDetailStore from 'components/PipelineDetails/store';
import {MyPipelineApi} from 'api/pipeline';
import uuidV4 from 'uuid/v4';
import {objectQuery} from 'services/helpers';
import uniqBy from 'lodash/uniqBy';

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineConfigureButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    resolvedMacros: PropTypes.object,
    runtimeArgs: PropTypes.array
  };

  state = {
    showModeless: false
  };

  getRuntimeArgumentsAndToggleModeless = () => {
    if (!this.state.showModeless) {
      const params = {
        namespace: getCurrentNamespace(),
        appId: PipelineDetailStore.getState().name
      };

      MyPipelineApi.fetchMacros(params)
        .combineLatest(MyPreferenceApi.getAppPreferences(params))
        .subscribe((res) => {
          let macrosSpec = res[0];
          let macrosMap = {};
          let macros = [];
          macrosSpec.map(ms => {
            if (objectQuery(ms, 'spec', 'properties', 'macros', 'lookupProperties')) {
              macros = macros.concat(ms.spec.properties.macros.lookupProperties);
            }
          });
          macros.forEach(macro => {
            macrosMap[macro] = '';
          });

          let currentAppPrefs = res[1];
          let resolvedMacros = getMacrosResolvedByPrefs(currentAppPrefs, macrosMap);

          PipelineConfigurationsStore.dispatch({
            type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
            payload: { resolvedMacros }
          });
          const getPairs = (map) => (
            Object
              .entries(map)
              .filter(([key]) => key.length)
              .map(([key, value]) => ({key, value, uniqueId: uuidV4()}))
          );
          let runtimeArgsPairs = getPairs(currentAppPrefs);
          let resolveMacrosPairs = getPairs(resolvedMacros);

          PipelineConfigurationsStore.dispatch({
            type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
            payload: {
              runtimeArgs: {
                pairs: uniqBy(runtimeArgsPairs.concat(resolveMacrosPairs), (pair) => pair.key)
              }
            }
          });
          this.toggleModeless();
        }, (err) => {
          console.log(err);
        }
      );
    } else {
      this.toggleModeless();
    }
  };

  toggleModeless = () => {
    this.setState({
      showModeless: !this.state.showModeless
    });
  };

  renderConfigureButton() {
    return (
      <div
        onClick={this.getRuntimeArgumentsAndToggleModeless}
        className="btn pipeline-action-btn pipeline-configure-btn"
      >
        <div className="btn-container">
          <IconSVG
            name="icon-sliders"
            className="configure-icon"
          />
          <div className="button-label">
            {T.translate(`${PREFIX}.configure`)}
          </div>
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className={classnames("pipeline-action-container pipeline-configure-container", {"active" : this.state.showModeless})}>
        {this.renderConfigureButton()}
        {
          this.state.showModeless ?
            <PipelineConfigurations
              onClose={this.toggleModeless}
              isDetailView={true}
              isBatch={this.props.isBatch}
              pipelineName={this.props.pipelineName}
            />
          :
            null
        }
      </div>
    );
  }
}
