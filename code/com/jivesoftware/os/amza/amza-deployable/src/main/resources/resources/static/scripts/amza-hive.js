var width = 960,
        height = 500,
        innerRadius = 40,
        outerRadius = 240;

var angle = d3.scale.ordinal().domain(d3.range(4)).rangePoints([0, 2 * Math.PI]),
        radius = d3.scale.linear().range([innerRadius, outerRadius]),
        color = d3.scale.category20c().domain(d3.range(20));

var links = [
    {source: {x: 0, y0: 0.9, y1: 1.0}, target: {x: 1, y0: 0.5, y1: 1.0}, group: 0},
    {source: {x: 0, y0: 0.7, y1: 0.9}, target: {x: 1, y0: 0.4, y1: 0.5}, group: 1},
    {source: {x: 0, y0: 0.4, y1: 0.7}, target: {x: 1, y0: 0.2, y1: 0.4}, group: 2},
    {source: {x: 0, y0: 0.0, y1: 0.4}, target: {x: 1, y0: 0.0, y1: 0.2}, group: 3},
    {source: {x: 1, y0: 0.8, y1: 1.0}, target: {x: 2, y0: 0.5, y1: 1.0}, group: 4},
    {source: {x: 1, y0: 0.5, y1: 0.8}, target: {x: 2, y0: 0.2, y1: 0.5}, group: 5},
    {source: {x: 1, y0: 0.3, y1: 0.5}, target: {x: 2, y0: 0.1, y1: 0.2}, group: 6},
    {source: {x: 1, y0: 0.0, y1: 0.3}, target: {x: 2, y0: 0.0, y1: 0.1}, group: 7},
    {source: {x: 2, y0: 0.8, y1: 1.0}, target: {x: 0, y0: 0.5, y1: 1.0}, group: 8},
    {source: {x: 2, y0: 0.5, y1: 0.8}, target: {x: 0, y0: 0.2, y1: 0.5}, group: 9},
    {source: {x: 2, y0: 0.1, y1: 0.5}, target: {x: 0, y0: 0.1, y1: 0.2}, group: 10},
    {source: {x: 2, y0: 0.0, y1: 0.1}, target: {x: 0, y0: 0.0, y1: 0.1}, group: 11}
];

var svg = d3.select("chart").append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

svg.selectAll(".link")
        .data(links)
        .enter().append("path")
        .attr("class", "link")
        .attr("d", d3.hive.link()
                .angle(function (d) {
                    return angle(d.x);
                })
                .startRadius(function (d) {
                    return radius(d.y0);
                })
                .endRadius(function (d) {
                    return radius(d.y1);
                }))
        .style("fill", function (d) {
            return color(d.group);
        });

svg.selectAll(".axis")
        .data(d3.range(3))
        .enter().append("g")
        .attr("class", "axis")
        .attr("transform", function (d) {
            return "rotate(" + degrees(angle(d)) + ")";
        })
        .selectAll("line")
        .data(["stroke", "fill"])
        .enter().append("line")
        .attr("class", function (d) {
            return d;
        })
        .attr("x1", radius.range()[0])
        .attr("x2", radius.range()[1]);

function degrees(radians) {
    return radians / Math.PI * 180 - 90;
}