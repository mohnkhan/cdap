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

import React, {Component} from 'react';
import {myProfileApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';

require('./ListViewInPipeline.scss');

export default class ProfilesListViewInPipeline extends Component {

  state = {
    profiles: [],
    loading: true,
    selectedProfile: null
  };

  componentWillMount() {
    myProfileApi.profiles({
      namespace: getCurrentNamespace()
    })
      .subscribe(profiles => {
        this.setState({
          loading: false,
          profiles
        });
      });
  }

  onProfileSelect = (profileName) => {
    this.setState({
      selectedProfile: profileName
    });
  };

  renderGridHeader = () => {
    return (
      <div className="grid-header">
        <div className="grid-item">
          <div></div>
          <strong>Profile Name</strong>
          <strong>Provider</strong>
          <strong>Scope</strong>
        </div>
      </div>
    );
  };

  renderGridBody = () => {
    return (
      <div className="grid-body">
        {
          this.state.profiles.map(profile => {
            return (
              <div
                className={classnames("grid-item", {
                  "active": this.state.selectedProfile === profile.name
                })}
                onClick={this.onProfileSelect.bind(this, profile.name)}
              >
                <div>
                  {
                    this.state.selectedProfile === profile.name ? (
                      <IconSVG name="icon-check" className="text-success" />
                    ) : null
                  }
                </div>
                <div>{profile.name}</div>
                <div>{profile.provisionerInfo.name}</div>
                <div>{profile.scope}</div>
              </div>
            );
          })
        }
      </div>
    );
  };

  render() {
    if (this.state.loading) {
      return (
        <div>
          <LoadingSVG />
        </div>
      );
    }
    return (
      <div className="profiles-list-view-on-pipeline">
        <strong> Select the compute profile you want to use to run this pipeline</strong>
        <div className="profiles-count text-right">{this.state.profiles.length} Compute Profiles</div>
        <div className="grid grid-container">
          {this.renderGridHeader()}
          {this.renderGridBody()}
        </div>
      </div>
    );
  }
}
