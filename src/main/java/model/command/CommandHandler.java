package model.command;


import dto.ActionDTO;
import service.Store;

import java.io.IOException;
import java.io.ObjectOutputStream;

public interface CommandHandler {
    void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException;
}
