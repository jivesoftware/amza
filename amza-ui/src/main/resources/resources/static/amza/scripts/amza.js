window.$ = window.jQuery;

window.amza = {};


amza.metrics = {
    input: {},
    requireFocus: true,
    html: null,
    init: function () {
        $stats = $('#stats');

        amza.metrics.poll();
    },
    poll: function () {
        $.ajax({
            type: "GET",
            url: "/amza/ui/metrics/stats/",
            dataType: "html",
            data: {
                partitionName: $('#metrics').data('partition-name'),
                exact: $('#metrics').data('partition-exact')
            },
            //contentType: "application/json",
            success: function (data) {
                amza.metrics.draw(data);
            },
            error: function () {
                //TODO error message
                console.log("error!");
            }
        });
    },
    draw: function (data) {
        $('#metrics').html(data);

        if (!amza.metrics.requireFocus || amza.windowFocused) {
            //amza.metrics.update();
        }
        setTimeout(amza.metrics.poll, 1000);
    }
};

amza.arc = {
    fetch: function () {
        var gotData;
        $.ajax("/amza/ui/arc", {
            method: "get",
            async: false,
            success: function (data) {
                gotData = data;
            },
            error: function () {
                alert('Arc suck!');
            }
        });

        return {data: function () {
                if (gotData)
                    return gotData;
            }};
    },
    init: function () {
        $.ajax("/amza/ui/arc", {
            method: "get",
            success: function (data) {
                amza.arc.build(data);
            },
            error: function () {
                alert('We suck!');
            }
        });
    },
    build: function (data) {

        var vis = new pv.Panel()
                .width(880)
                .height(310)
                .bottom(90);

        var arc = vis.add(pv.Layout.Arc)
                .nodes(data.nodes)
                .links(data.links)
                .sort(function (a, b) {
                    a.group == b.group
                            ? b.linkDegree - a.linkDegree
                            : b.group - a.group;
                });

        arc.link.add(pv.Line);

        arc.node.add(pv.Dot)
                .size(function (d) {
                    d.linkDegree + 4
                })
                .fillStyle(pv.Colors.category19().by(function (d) {
                    d.group
                }))
                .strokeStyle(function () {
                    this.fillStyle().darker()
                });

        arc.label.add(pv.Label)

        vis.render();

    }
};

amza.chord = {
    init: function () {
        $.ajax("/amza/ui/chord", {
            method: "get",
            success: function (data) {
                amza.chord.build(data);
            },
            error: function () {
                alert('We suck!');
            }
        });
    },
    build: function (matrix) {
        // From http://mkweb.bcgsc.ca/circos/guide/tables/

        var chord = d3.layout.chord()
                .padding(.05)
                .matrix(matrix);

        var width = 960,
                height = 500,
                innerRadius = Math.min(width, height) * .41,
                outerRadius = innerRadius * 1.1;

        var fill = d3.scale.ordinal()
                .domain(d3.range(4))
                .range(["#000000", "#FFDD89", "#957244", "#F26223"]);

        var svg = d3.select("#chord").append("svg")
                .attr("width", width)
                .attr("height", height)
                .append("g")
                .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

        svg.append("g").selectAll("path")
                .data(chord.groups)
                .enter().append("path")
                .style("fill", function (d) {
                    return fill(d.index);
                })
                .style("stroke", function (d) {
                    return fill(d.index);
                })
                .attr("d", d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius))
                .on("mouseover", amza.chord.fade(.1))
                .on("mouseout", amza.chord.fade(1));

        var ticks = svg.append("g").selectAll("g")
                .data(chord.groups)
                .enter().append("g").selectAll("g")
                .data(amza.chord.groupTicks)
                .enter().append("g")
                .attr("transform", function (d) {
                    return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
                            + "translate(" + outerRadius + ",0)";
                });

        ticks.append("line")
                .attr("x1", 1)
                .attr("y1", 0)
                .attr("x2", 5)
                .attr("y2", 0)
                .style("stroke", "#000");

        ticks.append("text")
                .attr("x", 8)
                .attr("dy", ".35em")
                .attr("transform", function (d) {
                    return d.angle > Math.PI ? "rotate(180)translate(-16)" : null;
                })
                .style("text-anchor", function (d) {
                    return d.angle > Math.PI ? "end" : null;
                })
                .text(function (d) {
                    return d.label;
                });

        svg.append("g")
                .attr("class", "chord")
                .selectAll("path")
                .data(chord.chords)
                .enter().append("path")
                .attr("d", d3.svg.chord().radius(innerRadius))
                .style("fill", function (d) {
                    return fill(d.target.index);
                })
                .style("opacity", 1);

        g.append("svg:text")
                .attr("x", 6)
                .attr("dy", 15)
                .filter(function (d) {
                    return d.value > 110;
                })
                .append("svg:textPath")
                .attr("xlink:href", function (d) {
                    return "#group" + d.index + "-" + j;
                })
                .text(function (d) {
                    return array[d.index].name;
                });

    },
    // Returns an array of tick angles and labels, given a group.
    groupTicks: function (d) {
        var k = (d.endAngle - d.startAngle) / d.value;
        return d3.range(0, d.value, 1000).map(function (v, i) {
            return {
                angle: v * k + d.startAngle,
                label: i % 5 ? null : v / 1000 + "k"
            };
        });
    },
// Returns an event handler for fading a given chord group.
    fade: function (opacity) {
        return function (g, i) {
            svg.selectAll(".chord path")
                    .filter(function (d) {
                        return d.source.index != i && d.target.index != i;
                    })
                    .transition()
                    .style("opacity", opacity);
        };
    }
};

amza.overview = {
    input: {},
    requireFocus: true,
    html: null,
    init: function () {
        $overview = $('#overview');

        amza.overview.poll();
    },
    poll: function () {
        $.ajax({
            type: "GET",
            url: "/amza/ui/metrics/overview",
            dataType: "html",
            data: {
                name: ""
            },
            //contentType: "application/json",
            success: function (data) {
                amza.overview.draw(data);
            },
            error: function () {
                //TODO error message
                console.log("error!");
            }
        });
    },
    draw: function (data) {
        $('#overview').html(data);

        if (!amza.overview.requireFocus || amza.windowFocused) {
            //amza.stats.update();
        }
        setTimeout(amza.overview.poll, 1000);
    }
};


amza.stats = {
    waves: {},
    data: {},
    initChart: function (which) {
        var $canvas = $(which);
        var ctx = which.getContext("2d");
        var id = $canvas.data('statsWaveId');
        if (!amza.stats.waves[id]) {
            var data = amza.stats.data[id];

            amza.stats.waves[id] = new Chart(ctx, {
                type: $canvas.data('statsWaveType'),
                data: data,
                options: {
                    maintainAspectRatio: false,
                    responsive: true,
                    legend: {
                        display: false
                    },
                    tooltips: {
                        enabled: true,
                        mode: 'label',
                        backgroundColor: 'rgba(100,100,100,0.8)'
                    },
                    gridLines: {
                        display: true,
                        color: "rgba(128,128,128,1)"
                    },
                    scaleLabel: {
                        fontColor: "rgba(200,200,200,1)"
                    },
                    scales: {
                        yAxes: [{
                                position: "right",
                                ticks: {
                                    beginAtZero: false
                                }
                            }]
                    },
                    animation: {
                        duration: 0
                    }
                }
            });
        }
        amza.stats.waves[id].update();

    },
    init: function () {

        $('.stats-wave').each(function (i) {
            amza.stats.initChart(this);
        });
    }
};

$(document).ready(function () {

    if ($('.stats-wave').length) {
        amza.stats.init();
    }

    $('.float-table-head').each(function (j, table) {
        $(table).floatThead({
            scrollingTop: 50
        });
    });

    if ($('#chord').length) {
        amza.chord.init();
    }
    if ($('#metrics').length) {
        amza.metrics.init();
    }
    if ($('#overview').length) {
        amza.overview.init();
    }

});
