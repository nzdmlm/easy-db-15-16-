package model.command;

import dto.ActionDTO;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import org.slf4j.Logger;
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class GetCommandHandler implements CommandHandler {
    private final Logger LOGGER;
    public GetCommandHandler(Logger LOGGER) {
        this.LOGGER = LOGGER;
    }

    @Override
    public void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException {
        String value = store.get(dto.getKey());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "get action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
        oos.writeObject(resp);
        oos.flush();
    }
}