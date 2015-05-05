/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.dashboard')
/* ------------------------------------------------------ */

  .controller('OpsCdapCtrl',
  function ($scope, opshelper) {
    var panels = [
   // Format:
   // [ Widget Title, context, [metricNames], line-type (options are in addwdgt-ctrl.js ]
      ['Router requests', '', ['system.request.received','system.response.client-error', 'system.response.successful'], 'c3-scatter'],
      ['Dataset Service', 'component.dataset~service', ['system.request.received','system.response.client-error','system.response.successful'],  'c3-scatter'],
      ['Transaction Commit', '', ['system.canCommit', 'system.commit', 'system.start.long', 'system.start.short'], 'c3-area-spline'],
      ['Transaction Latency', '', ['system.commit.latency', 'system.start.short.latency'], 'c3-area-spline'],
      ['System Error and Warnings', '', ['system.services.log.error', 'system.services.log.warn'], 'c3-area-step'],
      ['Explore Service', 'component.explore~service', ['system.request.received','system.response.successful'], 'c3-area-spline'],
      ['Events Processed', 'namespace.*', ['system.process.events.processed'], 'c3-line'],
      ['Bytes Store',   'namespace.*', ['system.dataset.store.bytes'],      'c3-line'],
      ['Dataset Read/Writes',     'namespace.*', ['system.dataset.store.writes' ,'system.dataset.store.reads'], 'c3-area-spline'],
      ['Containers Used', 'namespace.*', ['system.resources.used.containers', 'system.process.instance'], 'c3-area-step']
    ];

    $scope.currentBoard = opshelper.createBoardFromPanels(panels);
  })

  .controller('OpsAppsCtrl',
  function ($scope, $state, myHelpers, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.apps = [];

    dataSrc
      .request({
        _cdapNsPath: '/apps'
      },
      function (apps) {
        $scope.apps = apps;

        var m = ['vcores', 'containers', 'memory'];

        for (var i = 0; i < m.length; i++) {

          dataSrc
            .poll({
              _cdapPath: '/metrics/query' +
                '?context=namespace.system' +
                '&metric=system.resources.used.' +
                m[i] + '&groupBy=app',
              method: 'POST'
            }, setMetric);
        }

      });

    function setMetric(r) {

      angular.forEach($scope.apps, function (app) {
        angular.forEach(r.series, function (s) {
          if(app.id === s.grouping.app) {
            myHelpers.deepSet(
              app,
              'metric.' + s.metricName.split('.').pop(),
              s.data[0].value
            );
          }
        });
      });

    }

  })

/* ------------------------------------------------------ */

  .factory('opshelper', function (Widget) {
    function createWidget(title, context, metricNames, type) {
      return new Widget({title: title, type: type, isLive: true,
        metric: {
          context: context,
          names: metricNames,
          startTime: 'now-3600s',
          endTime: 'now',
          resolution: '1m'
        },
        interval: 15000
      });
    }

    function createBoardFromPanels(panels) {
      var widgets = [];
      panels.forEach(function(panel) {
        widgets.push(createWidget(panel[0], panel[1], panel[2], panel[3]));
      });
      // Note: It doesn't seem like this matters (as long as its high enough)
      var widgetsPerRow = 3;
      var columns = [];
      for (var i = 0; i < widgetsPerRow; i++) {
        columns.push([]);
      }
      for (var i = 0; i < widgets.length; i++) {
        columns[i % widgetsPerRow].push(widgets[i]);
      }
      // Note: title is not currently used in the view
      return {title : "System metrics", columns : columns};
    }

    return {
      createBoardFromPanels: createBoardFromPanels
    };
  })

  ;
