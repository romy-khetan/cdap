/*
 * Copyright © 2016 Cask Data, Inc.
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

function link (scope, element) {

  //Globals
  let width,
      height,
      paddingLeft,
      paddingRight,
      maxRange,
      sliderLimit,
      pinX,
      sliderX,
      timelineStack,
      startTime,
      endTime,
      pinOffset,
      handleWidth,
      handlePosition,
      firstRun = true;

  //Components
  let sliderHandle,
      pinHandle,
      sliderBrush,
      scrollPinBrush,
      xScale,
      timelineData,
      slide,
      slider,
      timescaleSvg,
      scrollPinSvg,
      xAxis,
      sliderBar,
      scrollNeedle,
      tooltipDiv;

  //Initialize charting
  scope.initialize = () => {

    //If chart already exists, remove it
    if(timescaleSvg){
      d3.selectAll('.timeline-container svg > *').remove();
      timescaleSvg.remove();
    }

    if(scrollPinSvg){
      scrollPinSvg.remove();
    }

    width = element.parent()[0].offsetWidth;
    height = 50;
    paddingLeft = 15;
    paddingRight = 15;
    handleWidth = 8;
    // maxRange = width - width of handle - width of needle;
    maxRange = width - 12;
    sliderLimit = maxRange;
    pinOffset = 13;
    pinX = 0;
    sliderX = 0;
    timelineStack = {};
    timelineData = scope.metadata;

    scope.plot();
  };

  /* ------------------- Plot Function ------------------- */
  scope.plot = function(){

    startTime = timelineData.qid.startTime*1000;
    endTime = timelineData.qid.endTime*1000;

    timescaleSvg = d3.select('.timeline-log-chart')
                .append('svg')
                .attr('width', width)
                .attr('height', height);

    //Set the Range and Domain
    xScale = d3.time.scale().range([0, (maxRange)]);
    xScale.domain([startTime, endTime]);

    var customTimeFormat = d3.time.format.multi([
      ['.%L', function(d) { return d.getMilliseconds(); }],
      [':%S', function(d) { return d.getSeconds(); }],
      ['%H:%M', function(d) { return d.getMinutes(); }],
      ['%H:%M', function(d) { return d.getHours(); }],
      ['%a %d', function(d) { return d.getDay() && d.getDate() !== 1; }],
      ['%b %d', function(d) { return d.getDate() !== 1; }],
      ['%B', function(d) { return d.getMonth(); }],
      ['%Y', function() { return true; }]
    ]);

    xAxis = d3.svg.axis().scale(xScale)
      .orient('bottom')
      .innerTickSize(-40)
      .outerTickSize(0)
      .tickPadding(7)
      .ticks(8)
      .tickFormat(customTimeFormat);

    generateEventCircles();
    renderBrushAndSlider();
  };

  // -------------------------Build Brush / Sliders------------------------- //
  function renderBrushAndSlider(){

    let pinScrollXPosition = xScale(scope.pinScrollingPosition);

    timescaleSvg.append('g')
      .attr('class', 'xaxis-bottom')
      .attr('transform', 'translate(' + handleWidth + ', ' + (height - 20) + ')')
      .call(xAxis);

    function updateScrollWithNewStart(startValue, currentPosition){
      if(startValue > currentPosition){
        scope.pinScrollingPosition = xScale.invert(startValue);
        scope.updatePin();
      }
    }

    //attach handler to brush
    sliderBrush = d3.svg.brush()
        .x(xScale)
        .on('brush', function(){
          if(d3.event.sourceEvent) {
            let val = d3.mouse(this)[0];
            if(val < 0){
              val = 0;
            }
            if(val > maxRange){
              val = maxRange;
            }
            handlePosition = val;
            sliderHandle.attr('x', val);
            sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
            updateScrollWithNewStart(val, pinScrollXPosition);
          }
        })
        .on('brushend', function() {
          if(d3.event.sourceEvent){
            let val = d3.mouse(this)[0];
            if(val < 0){
              val = 0;
            }
            if(val > maxRange){
              val = maxRange;
            }
            handlePosition = val;
            updateSlider(val);
            updateScrollWithNewStart(val, pinScrollXPosition);
          }
       });

    //Creates the top slider and trailing dark background
    sliderBar = timescaleSvg.append('g')
      .attr('class', 'slider leftSlider')
      .call(d3.svg.axis()
        .scale(xScale)
        .tickSize(0)
        .tickFormat(''))
      .select('.domain')
      .attr('class', 'fill-bar');

    slide = timescaleSvg.append('g')
          .attr('class', 'slider')
          .attr('transform' , 'translate(0,10)')
          .call(sliderBrush);

    if(firstRun){
      firstRun = false;
      scope.sliderBarPositionRefresh = xScale.invert(0);
      scope.Timeline.updateStartTimeInStore(xScale.invert(0));
    }
    let xValue = xScale(scope.sliderBarPositionRefresh);
    if(xValue < 0 || xValue > maxRange){
      xValue = 0;
    }

    sliderBar.attr('d', 'M0,0V0H' + xValue + 'V0');

    sliderHandle = slide.append('svg:image')
      .attr('width', handleWidth)
      .attr('height', 52)
      .attr('xlink:href', '/assets/img/sliderHandle.svg')
      .attr('x', xValue-1)
      .attr('y', -10);

    //Append the Top slider
    scrollPinBrush = d3.svg.brush()
        .x(xScale);

    scrollPinSvg = d3.select('.top-bar').append('svg')
        .attr('width', width)
        .attr('height', 20);

    scrollPinSvg.append('g')
        .attr('class', 'xaxis-top')
        .call(d3.svg.axis()
          .scale(xScale)
          .orient('bottom'))
      .select('.domain')
      .select( function() {
        return this.parentNode.appendChild(this.cloneNode(true));
      });

    slider = scrollPinSvg.append('g')
        .attr('class', 'slider')
        .attr('width', width)
        .call(scrollPinBrush);

    slider.select('.background')
      .attr('height', 15);

    let thePinScrollPosition = xScale(scope.pinScrollingPosition);

    pinHandle = slider.append('svg:image')
      .attr('width', 40)
      .attr('height', 60)
      .attr('xlink:href', '/assets/img/scrollPin.svg')
      .attr('x', thePinScrollPosition - pinOffset)
      .attr('y', 0)
      .on('mouseover', function() {
        tooltipDiv = d3.select('body').append('div')
          .attr('class', 'tooltip')
          .style('opacity', 0);

        //Reposition tooltip if overflows on the side of the page ; tooltip width is 250
        let overflowOffset = xScale(scope.pinScrollingPosition) + 250 > maxRange ? 250 : 0;

        tooltipDiv.transition()
          .duration(200)
          .style('opacity', 0.9)
          .attr('class', 'timeline-tooltip');
        tooltipDiv.html(scope.pinScrollingPosition)
          .style('left', (d3.event.pageX - overflowOffset) + 'px')
          .style('top', (d3.event.pageY - 28) + 'px');
      })
      .on('mouseout', function() {
        d3.selectAll('.timeline-tooltip').remove();
      });

    scrollNeedle = slide.append('line')
      .attr('x1', thePinScrollPosition + pinOffset - 6)
      .attr('x2', thePinScrollPosition + pinOffset - 6)
      .attr('y1', -10)
      .attr('y2', 40)
      .attr('stroke-width', 1)
      .attr('stroke', 'grey');
  }

  scope.updatePin = function () {
    let xPositionVal = xScale(scope.pinScrollingPosition);
    if(xPositionVal < 0 || xPositionVal > maxRange){
      return;
    }
    if(typeof pinHandle !== 'undefined'){

     if(xPositionVal < handlePosition){
      xPositionVal = handlePosition;
     }

     pinHandle.attr('x', xPositionVal - pinOffset + 1);
     scrollNeedle.attr('x1', xPositionVal + 8)
      .attr('x2', xPositionVal + 8);
    }
  };

  function updateSlider(val) {
    if(val < 0){
      val = 0;
    }
    if(val > sliderLimit){
      val = sliderLimit;
    }
    sliderX = val;

    sliderHandle.attr('x', val);
    sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
    scope.Timeline.updateStartTimeInStore(xScale.invert(val));
  }

  scope.updateSlider = updateSlider;

  var generateEventCircles = function (){
    let circleClass;

    if(timelineData.qid.series.length > 0){
      for(let i = 0; i < timelineData.qid.series.length; i++){

        switch(timelineData.qid.series[i].metricName){
          case 'system.app.log.error':
            circleClass = 'red-circle';
            break;
          case 'system.app.log.warn':
            circleClass = 'yellow-circle';
            break;
          default:
            circleClass = 'other-circle';
            break;
        }

        for(let j = 0; j < timelineData.qid.series[i].data.length; j++){
          let currentItem = timelineData.qid.series[i].data[j];
          let xVal = Math.floor(xScale(currentItem.time));
          let numEvents = currentItem.value;

          if(typeof timelineStack[xVal] === 'undefined') {
            timelineStack[xVal] = 0;
          }

          //plot events until vertical limit (5)
          for(var k = 0; k < numEvents && timelineStack[xVal] < 5; k++){
            timelineStack[xVal]++;

            //Append the circle
            if(currentItem){
              timescaleSvg.append('circle').attr('cx', xScale(currentItem.time *1000)).attr('cy', (timelineStack[xVal])*7).attr('r', 2).attr('class', circleClass);
            }
          }
        }
      }
    }
  };
  scope.generateEventCircles = generateEventCircles;
}

angular.module(PKG.name + '.commons')
.directive('myTimeline', function() {
  return {
    templateUrl: 'timeline/timeline.html',
    scope: {
      timelineData: '=?',
      namespaceId: '@',
      appId: '@',
      programType: '@',
      programId: '@',
      runId: '@'
    },
    link: link,
    bindToController: true,
    controller: 'TimelineController',
    controllerAs: 'Timeline'
  };
});
