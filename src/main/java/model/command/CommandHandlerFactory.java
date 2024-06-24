package model.command;

import dto.ActionTypeEnum;
import org.slf4j.Logger;

public class CommandHandlerFactory {
    public static CommandHandler getHandler(ActionTypeEnum type, Logger LOGGER) {
        switch (type) {
            case GET:
                return new GetCommandHandler(LOGGER);
            case SET:
                return new SetCommandHandler(LOGGER);
            case RM:
                return new RmCommandHandler();
            default:
                throw new IllegalArgumentException("Unsupported action type: " + type);
        }
    }
}
