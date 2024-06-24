package model.command;

import dto.ActionDTO;
import service.Store;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class RmCommandHandler implements CommandHandler {
    @Override
    public void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException {
        store.rm(dto.getKey());
    }
}