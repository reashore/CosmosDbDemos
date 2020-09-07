function spHelloWorld() {
	const context = getContext();
	const response = context.getResponse();

	response.setBody('Greetings from the Cosmos DB server!');
}
