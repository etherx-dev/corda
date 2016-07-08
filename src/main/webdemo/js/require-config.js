'use strict';

require.config({
	paths: {
		angular: 'bower_components/angular/angular',
		angularRoute: 'bower_components/angular-route/angular-route',
		fcsaNumber: 'bower_components/angular-fcsa-number/src/fcsaNumber',
		jquery: 'bower_components/jquery/dist/jquery',
		semantic: 'semantic/semantic',
		lodash: 'bower_components/lodash/lodash',
		maskedInput: 'bower_components/jquery.maskedinput/dist/jquery.maskedinput'
	},
	shim: {
		'angular' : {'exports' : 'angular'},
		'angularRoute': ['angular'],
		'fcsaNumber': ['angular'],
		'semantic': ['jquery'],
		'maskedInput': ['jquery']
	},
	priority: [
		"angular"
	],
	deps: [],
	callback: null,
	baseUrl: '',
});

require(['angular', 'js/app'], (angular, app) => {
    var $html = angular.element(document.getElementsByTagName('html')[0])
    angular.element().ready(function() {
        // bootstrap the app manually
        angular.bootstrap(document, ['irsViewer']);
    });
});