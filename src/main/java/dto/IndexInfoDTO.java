package dto;

import model.command.CommandPos;

public class IndexInfoDTO {
    String path;
    CommandPos commandpos;

    public IndexInfoDTO(String path, CommandPos commandpos) {
        this.path = path;
        this.commandpos = commandpos;
    }

    public String getPath() {
        return path;
    }

    public CommandPos getCommandpos() {
        return commandpos;
    }

}
