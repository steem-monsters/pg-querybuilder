"use strict";
class Person {
    constructor() {
        this.firstName = 'John';
        this.lastName = 'Doe';
    }
}
class Factory {
    create(type) {
        return new type();
    }
}
let factory = new Factory();
let person = factory.create(Person);
console.log(JSON.stringify(person));
//# sourceMappingURL=test2.js.map