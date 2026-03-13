const Director = require("./director/Director");

module.exports.loop = function () {

    try {

        const director = new Director();
        director.run();

    } catch (err) {

        console.log("Tick failure:", err.stack);

    }

};