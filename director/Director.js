const { buildPipeline } = require("./pipelineRegistry");
const ActionQueue = require("../runtime/ActionQueue");
const ActionResolver = require("../runtime/ActionResolver");
const colonyConfig = require("../config/colonyConfig");

class Director {

    constructor() {
        this.pipeline = buildPipeline();
    }

    run() {

        const actionQueue = new ActionQueue();

        const context = {
            tick: Game.time,
            worldState: null,
            config: colonyConfig,
            logger: console
        };

        for (const stage of this.pipeline) {

            const expert = stage.expert;

            if (!expert) {
                continue;
            }

            if (expert.analyze) {
                expert.analyze(context);
            }

            if (expert.decide) {
                expert.decide(context);
            }

            if (expert.act) {
                expert.act(context, actionQueue);
            }

            if (stage.name === "WorldStateExpert") {
                context.worldState = expert.worldState;
            }

        }

        const resolver = new ActionResolver();

        resolver.resolve(actionQueue.getAll(), context);

    }

}

module.exports = Director;