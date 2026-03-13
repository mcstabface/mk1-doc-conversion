const WorldStateExpert = require("../experts/world/WorldStateExpert");
const HarvestExpert = require("../experts/harvest/HarvestExpert");

function buildPipeline() {
    return [
        {
            name: "WorldStateExpert",
            expert: new WorldStateExpert(),
            alwaysRun: true
        },
        {
            name: "HarvestExpert",
            expert: new HarvestExpert(),
            alwaysRun: true
        }
    ];
}

module.exports = {
    buildPipeline
};